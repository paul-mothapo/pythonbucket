use anyhow::{Context, Result, bail};
use chrono::{DateTime, Duration, NaiveDate, SecondsFormat, Utc};
use clap::{Parser, ValueEnum};
use reqwest::StatusCode;
use reqwest::blocking::Client;
use reqwest::header::{ACCEPT, HeaderMap, HeaderValue, USER_AGENT};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::thread;
use std::time::Duration as StdDuration;

const PER_PAGE: u32 = 100;
const DAILY_GOAL: usize = 500;
const DEFAULT_MAX_INACTIVE_MONTHS: u32 = 18;
const ACTIVE_WINDOW_DAYS: i64 = 180;
const TOP_README_LIMIT: usize = 500;
const SECTION_LIMIT: usize = 15;
const STATE_FILE: &str = "python_bucket_state.json";
const README_FILE: &str = "README.md";
const OUTPUT_JSON: &str = "python_projects.json";

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum Mode {
    Once,
    Loop,
}

#[derive(Parser, Debug, Clone)]
#[command(
    name = "pythonbucket",
    about = "Collect Python repositories from GitHub and update local outputs."
)]
struct RunConfig {
    #[arg(value_enum, default_value_t = Mode::Once)]
    mode: Mode,
    #[arg(long, default_value_t = DAILY_GOAL)]
    goal: usize,
    #[arg(long, default_value_t = 0)]
    min_stars: u64,
    #[arg(long)]
    include_forks: bool,
    #[arg(long)]
    include_archived: bool,
    #[arg(long, default_value = "")]
    query: String,
    #[arg(long, default_value_t = DEFAULT_MAX_INACTIVE_MONTHS)]
    max_inactive_months: u32,
    #[arg(long)]
    reset_state: bool,
    #[arg(long)]
    readme_only: bool,
    #[arg(long)]
    json_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(default)]
struct Filters {
    query: String,
    min_stars: u64,
    include_forks: bool,
    include_archived: bool,
    max_inactive_months: u32,
    cutoff_date: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
struct LastRun {
    started_at: Option<String>,
    completed_at: Option<String>,
    new_repo_count: usize,
    new_repo_ids: Vec<u64>,
    filters: Filters,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
struct State {
    page: u32,
    collected: usize,
    seen_ids: Vec<u64>,
    last_run: LastRun,
}

impl Default for State {
    fn default() -> Self {
        Self {
            page: 1,
            collected: 0,
            seen_ids: Vec::new(),
            last_run: LastRun::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
struct Repo {
    id: u64,
    name: String,
    description: String,
    stars: u64,
    forks: u64,
    watchers: u64,
    open_issues: u64,
    url: String,
    homepage: String,
    license: Option<String>,
    topics: Vec<String>,
    created_at: Option<String>,
    updated_at: Option<String>,
    pushed_at: String,
    archived: bool,
    fork: bool,
    fetched_at: String,
}

#[derive(Debug, Deserialize)]
struct SearchResponse {
    items: Vec<SearchRepo>,
}

#[derive(Debug, Deserialize)]
#[serde(default)]
struct SearchRepo {
    id: u64,
    name: String,
    full_name: String,
    description: Option<String>,
    stargazers_count: u64,
    forks_count: u64,
    watchers_count: u64,
    open_issues_count: u64,
    url: Option<String>,
    html_url: Option<String>,
    homepage: Option<String>,
    license: Option<SearchLicense>,
    topics: Vec<String>,
    created_at: Option<String>,
    updated_at: Option<String>,
    pushed_at: Option<String>,
    archived: bool,
    fork: bool,
}

impl Default for SearchRepo {
    fn default() -> Self {
        Self {
            id: 0,
            name: String::new(),
            full_name: String::new(),
            description: None,
            stargazers_count: 0,
            forks_count: 0,
            watchers_count: 0,
            open_issues_count: 0,
            url: None,
            html_url: None,
            homepage: None,
            license: None,
            topics: Vec::new(),
            created_at: None,
            updated_at: None,
            pushed_at: None,
            archived: false,
            fork: false,
        }
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct SearchLicense {
    spdx_id: Option<String>,
    name: Option<String>,
}

#[derive(Debug, Clone)]
struct SummaryStats {
    total: usize,
    median_stars: u64,
    active_recently: usize,
    licensed: usize,
}

#[derive(Debug)]
struct CollectResult {
    all_repos: Vec<Repo>,
    new_repos: Vec<Repo>,
    state: State,
    filters: Filters,
}

#[derive(Copy, Clone)]
enum Column {
    Rank,
    Project,
    Stars,
    Forks,
    Updated,
    Description,
}

impl Column {
    fn header(self) -> &'static str {
        match self {
            Self::Rank => "#",
            Self::Project => "Project",
            Self::Stars => "Stars",
            Self::Forks => "Forks",
            Self::Updated => "Updated",
            Self::Description => "Description",
        }
    }
}

fn utc_now() -> DateTime<Utc> {
    Utc::now()
}

fn isoformat_utc(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn cutoff_date(months: u32) -> Option<NaiveDate> {
    if months == 0 {
        None
    } else {
        Some((utc_now() - Duration::days(months as i64 * 30)).date_naive())
    }
}

fn parse_timestamp(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|timestamp| timestamp.with_timezone(&Utc))
}

fn normalize_topics(topics: Vec<String>) -> Vec<String> {
    let mut cleaned = BTreeSet::new();
    for topic in topics {
        let trimmed = topic.trim();
        if !trimmed.is_empty() {
            cleaned.insert(trimmed.to_string());
        }
    }
    cleaned.into_iter().collect()
}

fn normalize_repo_url(url: &str) -> String {
    let trimmed = url.trim();
    let api_prefix = "https://api.github.com/repos/";
    let web_prefix = "https://github.com/repos/";
    if let Some(rest) = trimmed.strip_prefix(api_prefix) {
        return format!("https://github.com/{rest}");
    }
    if let Some(rest) = trimmed.strip_prefix(web_prefix) {
        return format!("https://github.com/{rest}");
    }
    trimmed.to_string()
}

fn repo_html_url(html_url: Option<&str>, url: Option<&str>) -> String {
    if let Some(value) = html_url {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    normalize_repo_url(url.unwrap_or_default())
}

fn license_name(license: &Option<SearchLicense>) -> Option<String> {
    let license = license.as_ref()?;
    license
        .spdx_id
        .as_ref()
        .or(license.name.as_ref())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn repo_from_search(item: SearchRepo) -> Repo {
    let fetched_at = isoformat_utc(utc_now());
    let updated_at = item.updated_at.clone();
    let pushed_at = item
        .pushed_at
        .clone()
        .or_else(|| updated_at.clone())
        .unwrap_or_else(|| fetched_at.clone());

    Repo {
        id: item.id,
        name: if item.name.trim().is_empty() {
            item.full_name
        } else {
            item.name
        },
        description: item.description.unwrap_or_default().trim().to_string(),
        stars: item.stargazers_count,
        forks: item.forks_count,
        watchers: item.watchers_count,
        open_issues: item.open_issues_count,
        url: repo_html_url(item.html_url.as_deref(), item.url.as_deref()),
        homepage: item.homepage.unwrap_or_default().trim().to_string(),
        license: license_name(&item.license),
        topics: normalize_topics(item.topics),
        created_at: item.created_at,
        updated_at,
        pushed_at,
        archived: item.archived,
        fork: item.fork,
        fetched_at,
    }
}

fn normalize_repo(mut repo: Repo) -> Repo {
    if repo.name.trim().is_empty() {
        repo.name = "unknown".to_string();
    }
    repo.description = repo.description.trim().to_string();
    repo.homepage = repo.homepage.trim().to_string();
    repo.url = normalize_repo_url(&repo.url);
    repo.topics = normalize_topics(repo.topics);
    if repo.fetched_at.trim().is_empty() {
        repo.fetched_at = isoformat_utc(utc_now());
    }
    if repo.pushed_at.trim().is_empty() {
        repo.pushed_at = repo
            .updated_at
            .clone()
            .unwrap_or_else(|| repo.fetched_at.clone());
    }
    repo
}

fn build_filters(config: &RunConfig) -> Filters {
    Filters {
        query: config.query.trim().to_string(),
        min_stars: config.min_stars,
        include_forks: config.include_forks,
        include_archived: config.include_archived,
        max_inactive_months: config.max_inactive_months,
        cutoff_date: cutoff_date(config.max_inactive_months).map(|date| date.to_string()),
    }
}

fn filters_changed(previous: &Filters, current: &Filters) -> bool {
    previous.query.trim() != current.query.trim()
        || previous.min_stars != current.min_stars
        || previous.include_forks != current.include_forks
        || previous.include_archived != current.include_archived
        || previous.max_inactive_months != current.max_inactive_months
}

fn has_saved_filters(filters: &Filters) -> bool {
    !filters.query.trim().is_empty()
        || filters.min_stars != 0
        || filters.include_forks
        || filters.include_archived
        || filters.max_inactive_months != 0
        || filters.cutoff_date.is_some()
}

fn build_search_query(config: &RunConfig) -> String {
    let mut parts = vec!["language:python".to_string()];
    if !config.query.trim().is_empty() {
        parts.push(config.query.trim().to_string());
    }
    if config.min_stars > 0 {
        parts.push(format!("stars:>={}", config.min_stars));
    }
    if !config.include_forks {
        parts.push("fork:false".to_string());
    }
    if !config.include_archived {
        parts.push("archived:false".to_string());
    }
    if let Some(cutoff) = cutoff_date(config.max_inactive_months) {
        parts.push(format!("pushed:>={cutoff}"));
    }
    parts.join(" ")
}

fn repo_matches_filters(repo: &Repo, config: &RunConfig) -> bool {
    if repo.stars < config.min_stars {
        return false;
    }
    if !config.include_forks && repo.fork {
        return false;
    }
    if !config.include_archived && repo.archived {
        return false;
    }
    if let Some(cutoff) = cutoff_date(config.max_inactive_months) {
        if let Some(pushed_at) = parse_timestamp(&repo.pushed_at) {
            if pushed_at.date_naive() < cutoff {
                return false;
            }
        }
    }
    true
}

fn default_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        ACCEPT,
        HeaderValue::from_static("application/vnd.github+json"),
    );
    headers.insert(USER_AGENT, HeaderValue::from_static("pythonbucket/0.1"));
    headers
}

fn build_client() -> Result<Client> {
    Client::builder()
        .default_headers(default_headers())
        .build()
        .context("failed to build HTTP client")
}

fn search_python_repos(client: &Client, page: u32, config: &RunConfig) -> Result<Vec<SearchRepo>> {
    let query = build_search_query(config);

    loop {
        let response = client
            .get("https://api.github.com/search/repositories")
            .query(&[
                ("q", query.as_str()),
                ("sort", "stars"),
                ("order", "desc"),
                ("per_page", &PER_PAGE.to_string()),
                ("page", &page.to_string()),
            ])
            .send()
            .context("failed to query GitHub search API")?;

        if response.status() == StatusCode::FORBIDDEN {
            let wait_seconds = response
                .headers()
                .get("x-ratelimit-reset")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<i64>().ok())
                .map(|reset| (reset - utc_now().timestamp()).max(5) as u64)
                .unwrap_or(60);
            println!("  Rate limited. Sleeping {wait_seconds}s ...");
            thread::sleep(StdDuration::from_secs(wait_seconds));
            continue;
        }

        let response = response
            .error_for_status()
            .context("GitHub search API returned an error")?;
        let body: SearchResponse = response
            .json()
            .context("failed to parse GitHub search response")?;
        return Ok(body.items);
    }
}

fn atomic_write(path: &str, content: &str) -> Result<()> {
    let target = Path::new(path);
    let directory = target.parent().unwrap_or_else(|| Path::new("."));
    let file_name = target
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("output");
    let temp_path = directory.join(format!(".{file_name}.{}.tmp", std::process::id()));

    fs::write(&temp_path, content)
        .with_context(|| format!("failed to write temporary file {}", temp_path.display()))?;
    if target.exists() {
        fs::remove_file(target)
            .with_context(|| format!("failed to replace {}", target.display()))?;
    }
    fs::rename(&temp_path, target)
        .with_context(|| format!("failed to move {} into place", temp_path.display()))?;
    Ok(())
}

fn atomic_write_json<T: Serialize>(path: &str, value: &T) -> Result<()> {
    let mut content = serde_json::to_string_pretty(value)
        .with_context(|| format!("failed to serialize {path}"))?;
    content.push('\n');
    atomic_write(path, &content)
}

fn load_state() -> Result<State> {
    if !Path::new(STATE_FILE).exists() {
        return Ok(State::default());
    }
    let content = fs::read_to_string(STATE_FILE).context("failed to read state file")?;
    let state = serde_json::from_str::<State>(&content).context("failed to parse state file")?;
    Ok(state)
}

fn save_state(state: &State) -> Result<()> {
    atomic_write_json(STATE_FILE, state)
}

fn load_repos() -> Result<Vec<Repo>> {
    if !Path::new(OUTPUT_JSON).exists() {
        return Ok(Vec::new());
    }
    let content = fs::read_to_string(OUTPUT_JSON).context("failed to read repo database")?;
    let repos =
        serde_json::from_str::<Vec<Repo>>(&content).context("failed to parse repo database")?;
    Ok(repos.into_iter().map(normalize_repo).collect())
}

fn format_date(value: &str) -> String {
    parse_timestamp(value)
        .map(|timestamp| timestamp.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn escape_markdown(text: &str) -> String {
    text.replace('|', "\\|")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn truncate(text: &str, limit: usize) -> String {
    let mut chars = text.chars();
    let truncated: String = chars.by_ref().take(limit).collect();
    if chars.next().is_some() && limit > 3 {
        format!(
            "{}...",
            truncated
                .chars()
                .take(limit - 3)
                .collect::<String>()
                .trim_end()
        )
    } else {
        truncated
    }
}

fn render_repo_table(repos: &[Repo], columns: &[Column]) -> Vec<String> {
    let mut lines = vec![
        format!(
            "| {} |",
            columns
                .iter()
                .map(|column| column.header())
                .collect::<Vec<_>>()
                .join(" | ")
        ),
        format!("| {} |", vec!["---"; columns.len()].join(" | ")),
    ];

    for (index, repo) in repos.iter().enumerate() {
        let row = columns
            .iter()
            .map(|column| match column {
                Column::Rank => (index + 1).to_string(),
                Column::Project => format!("[{}]({})", escape_markdown(&repo.name), repo.url),
                Column::Stars => repo.stars.to_string(),
                Column::Forks => repo.forks.to_string(),
                Column::Updated => format_date(&repo.pushed_at),
                Column::Description => escape_markdown(&truncate(&repo.description, 90)),
            })
            .collect::<Vec<_>>();
        lines.push(format!("| {} |", row.join(" | ")));
    }

    lines
}

fn summary_stats(repos: &[Repo]) -> SummaryStats {
    if repos.is_empty() {
        return SummaryStats {
            total: 0,
            median_stars: 0,
            active_recently: 0,
            licensed: 0,
        };
    }

    let mut stars = repos.iter().map(|repo| repo.stars).collect::<Vec<_>>();
    stars.sort_unstable();
    let middle = stars.len() / 2;
    let median_stars = if stars.len() % 2 == 0 {
        (stars[middle - 1] + stars[middle]) / 2
    } else {
        stars[middle]
    };

    let active_cutoff = utc_now() - Duration::days(ACTIVE_WINDOW_DAYS);
    let active_recently = repos
        .iter()
        .filter(|repo| {
            parse_timestamp(&repo.pushed_at).is_some_and(|timestamp| timestamp >= active_cutoff)
        })
        .count();
    let licensed = repos.iter().filter(|repo| repo.license.is_some()).count();

    SummaryStats {
        total: repos.len(),
        median_stars,
        active_recently,
        licensed,
    }
}

fn generate_readme(
    repos: &[Repo],
    new_repos: &[Repo],
    state: &State,
    filters: &Filters,
) -> Result<()> {
    let now = utc_now();
    let now_display = now.format("%Y-%m-%d %H:%M UTC").to_string();
    let now_badge = now_display.replace(' ', "%20").replace(':', "%3A");
    let stats = summary_stats(repos);

    let mut recent = repos.to_vec();
    recent.sort_by_key(|repo| parse_timestamp(&repo.pushed_at));
    recent.reverse();
    recent.truncate(SECTION_LIMIT);

    let top = repos
        .iter()
        .take(TOP_README_LIMIT)
        .cloned()
        .collect::<Vec<_>>();

    let mut lines = vec![
        "# Python Bucket".to_string(),
        String::new(),
        format!(
            "![Last Updated](https://img.shields.io/badge/Updated%20on-{now_badge}-brightgreen)"
        ),
        format!(
            "![Total Projects](https://img.shields.io/badge/Projects%20Indexed-{}-blue)",
            stats.total
        ),
        String::new(),
        format!("## Updated on {now_display}"),
        String::new(),
        "> Automated collection of open-source Python projects from GitHub.".to_string(),
        "> Built as a filtered discovery index for active Python repositories.".to_string(),
        String::new(),
        "## Run Summary".to_string(),
        String::new(),
        "| Metric | Value |".to_string(),
        "| --- | --- |".to_string(),
        format!("| Total projects indexed | {} |", stats.total),
        format!("| New repos added this run | {} |", new_repos.len()),
        format!("| Median stars | {} |", stats.median_stars),
        format!(
            "| Active in last {ACTIVE_WINDOW_DAYS} days | {} |",
            stats.active_recently
        ),
        format!("| Repos with a detected license | {} |", stats.licensed),
        String::new(),
        "## Filters".to_string(),
        String::new(),
        "| Setting | Value |".to_string(),
        "| --- | --- |".to_string(),
        format!(
            "| Additional query | `{}` |",
            if filters.query.is_empty() {
                "-"
            } else {
                filters.query.as_str()
            }
        ),
        format!("| Minimum stars | {} |", filters.min_stars),
        format!(
            "| Forks included | {} |",
            if filters.include_forks { "yes" } else { "no" }
        ),
        format!(
            "| Archived repos included | {} |",
            if filters.include_archived {
                "yes"
            } else {
                "no"
            }
        ),
        format!(
            "| Inactive cutoff | {} |",
            filters.cutoff_date.as_deref().unwrap_or("disabled")
        ),
        format!("| Last saved page | {} |", state.page),
    ];

    if !new_repos.is_empty() {
        let mut section = new_repos.to_vec();
        section.sort_by(|left, right| right.stars.cmp(&left.stars));
        section.truncate(SECTION_LIMIT);
        lines.push(String::new());
        lines.push("## New Repos Added This Run".to_string());
        lines.push(String::new());
        lines.extend(render_repo_table(
            &section,
            &[
                Column::Project,
                Column::Stars,
                Column::Updated,
                Column::Description,
            ],
        ));
    }

    if !recent.is_empty() {
        lines.push(String::new());
        lines.push("## Most Recently Updated Repos".to_string());
        lines.push(String::new());
        lines.extend(render_repo_table(
            &recent,
            &[
                Column::Project,
                Column::Stars,
                Column::Updated,
                Column::Description,
            ],
        ));
    }

    lines.push(String::new());
    lines.push(format!("## Top {} Python Projects", top.len()));
    lines.push(String::new());
    lines.extend(render_repo_table(
        &top,
        &[
            Column::Rank,
            Column::Project,
            Column::Stars,
            Column::Forks,
            Column::Updated,
            Column::Description,
        ],
    ));
    lines.push(String::new());
    lines.push("---".to_string());
    lines.push(String::new());
    lines.push("## Usage".to_string());
    lines.push(String::new());
    lines.push("```bash".to_string());
    lines.push("cargo run --release -- once --goal 500".to_string());
    lines.push(
        r#"cargo run --release -- once --query "topic:machine-learning" --min-stars 200"#
            .to_string(),
    );
    lines.push("cargo run --release -- once --readme-only".to_string());
    lines.push("```".to_string());
    lines.push(String::new());
    lines.push("_Generated by Python Bucket - run `cargo run -- --help` for options._".to_string());

    atomic_write(README_FILE, &(lines.join("\n") + "\n"))?;
    println!("README.md written with {} projects.", top.len());
    Ok(())
}

fn collect(client: &Client, config: &RunConfig) -> Result<CollectResult> {
    let mut existing_map = HashMap::<u64, Repo>::new();
    for repo in load_repos()? {
        if repo.id != 0 && repo_matches_filters(&repo, config) {
            existing_map.insert(repo.id, repo);
        }
    }

    let mut state = if config.reset_state {
        let mut reset = State::default();
        reset.seen_ids = existing_map.keys().copied().collect();
        reset.seen_ids.sort_unstable();
        reset
    } else {
        load_state()?
    };
    let current_filters = build_filters(config);

    if !config.reset_state
        && has_saved_filters(&state.last_run.filters)
        && filters_changed(&state.last_run.filters, &current_filters)
    {
        println!("Filters changed since the last run. Resetting pagination for the new query.");
        let seen_ids = existing_map.keys().copied().collect::<Vec<_>>();
        state = State::default();
        state.seen_ids = seen_ids;
    }

    let mut seen_ids = state.seen_ids.iter().copied().collect::<HashSet<_>>();
    let mut new_repos = Vec::<Repo>::new();
    let mut page = state.page;
    let started_at = isoformat_utc(utc_now());

    println!(
        "Python Bucket - collecting up to {} repos (starting page {})",
        config.goal, page
    );
    println!("Search query: {}", build_search_query(config));

    while new_repos.len() < config.goal {
        print!("Fetching page {page} ... ");
        let items = search_python_repos(client, page, config)?;
        if items.is_empty() {
            println!("no more results.");
            break;
        }

        let mut added = 0usize;
        for item in items {
            let repo = repo_from_search(item);
            if !repo_matches_filters(&repo, config) {
                continue;
            }
            if seen_ids.insert(repo.id) {
                new_repos.push(repo.clone());
                added += 1;
            }
            existing_map.insert(repo.id, repo);
            if new_repos.len() >= config.goal {
                break;
            }
        }

        println!("+{added} new  (total new today: {})", new_repos.len());
        page += 1;

        if new_repos.len() >= config.goal {
            break;
        }
        thread::sleep(StdDuration::from_millis(2200));
    }

    let mut all_repos = existing_map.into_values().collect::<Vec<_>>();
    all_repos.sort_by(|left, right| {
        right
            .stars
            .cmp(&left.stars)
            .then_with(|| left.name.cmp(&right.name))
    });

    state.page = page;
    state.collected += new_repos.len();
    state.seen_ids = seen_ids.into_iter().collect();
    state.seen_ids.sort_unstable();
    state.last_run = LastRun {
        started_at: Some(started_at),
        completed_at: Some(isoformat_utc(utc_now())),
        new_repo_count: new_repos.len(),
        new_repo_ids: new_repos.iter().map(|repo| repo.id).collect(),
        filters: current_filters.clone(),
    };

    save_state(&state)?;
    atomic_write_json(OUTPUT_JSON, &all_repos)?;

    println!(
        "\nCollected {} new repos. Total in database: {}",
        new_repos.len(),
        all_repos.len()
    );

    Ok(CollectResult {
        all_repos,
        new_repos,
        state,
        filters: current_filters,
    })
}

fn run_once(client: &Client, config: &RunConfig) -> Result<()> {
    if config.reset_state {
        save_state(&State::default())?;
        println!("State reset to page 1.");
    }

    if config.readme_only {
        let state = load_state()?;
        let mut repos = load_repos()?;
        repos.sort_by(|left, right| {
            right
                .stars
                .cmp(&left.stars)
                .then_with(|| left.name.cmp(&right.name))
        });
        let filters = if has_saved_filters(&state.last_run.filters) {
            state.last_run.filters.clone()
        } else {
            build_filters(config)
        };
        generate_readme(&repos, &[], &state, &filters)?;
        return Ok(());
    }

    let result = collect(client, config)?;
    if !config.json_only {
        generate_readme(
            &result.all_repos,
            &result.new_repos,
            &result.state,
            &result.filters,
        )?;
    }
    Ok(())
}

fn run_loop(client: &Client, config: &RunConfig, interval_hours: u64) -> Result<()> {
    loop {
        println!("\n============================================================");
        println!("{} - starting scheduled run", isoformat_utc(utc_now()));
        println!("============================================================");
        run_once(client, config)?;
        let next_run = utc_now() + Duration::hours(interval_hours as i64);
        println!(
            "\nNext run at {} - sleeping ...\n",
            next_run.format("%Y-%m-%d %H:%M UTC")
        );
        thread::sleep(StdDuration::from_secs(interval_hours * 3600));
    }
}

fn validate_config(config: &RunConfig) -> Result<()> {
    if config.readme_only && config.json_only {
        bail!("--readme-only and --json-only cannot be used together");
    }
    Ok(())
}

fn main() -> Result<()> {
    let config = RunConfig::parse();
    validate_config(&config)?;
    let client = build_client()?;

    match config.mode {
        Mode::Loop => run_loop(&client, &config, 24),
        Mode::Once => run_once(&client, &config),
    }
}
