#!/usr/bin/env python3
import json
import os
import urllib.request
from datetime import datetime, timezone

# Environment
REPO = os.environ["GITHUB_REPOSITORY"]
GH_TOKEN = os.environ["GH_TOKEN"]
DISCORD_WEBHOOK = os.environ["DISCORD_WEBHOOK"]
EVENT_NAME = os.environ.get("EVENT_NAME", "")
EVENT_ACTION = os.environ.get("EVENT_ACTION", "")
PR_NUMBER = os.environ.get("PR_NUMBER", "")

TEAM_FILE = os.environ.get("TEAM_FILE", ".github/pr-review-team.txt")
MIN_REVIEWS = 2
REMINDER_DELAY_HOURS = 2
REMINDER_LABEL = "reminded"

def github_api(path, method="GET", data=None):
    url = f"https://api.github.com/{path.lstrip('/')}"
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {GH_TOKEN}",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "github-pr-review-bot"
    }
    
    if data:
        data = json.dumps(data).encode("utf-8")
        headers["Content-Type"] = "application/json"
        
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request) as response:
            body = response.read()
            return json.loads(body.decode("utf-8")) if body else None
    except urllib.error.HTTPError:
        return None  # Ignore 404s for missing labels, etc.

def discord_message(content):
    payload = json.dumps({"content": content, "allowed_mentions": {"parse": ["users", "everyone"]}})
    request = urllib.request.Request(DISCORD_WEBHOOK, data=payload.encode("utf-8"), headers={
        "Content-Type": "application/json",
        "User-Agent": "github-pr-review-bot"
    })
    urllib.request.urlopen(request)

def load_team():
    team = {}
    if os.path.exists(TEAM_FILE):
        with open(TEAM_FILE, "r") as f:
            for line in f:
                if "=" in line and not line.startswith("#"):
                    gh, discord = line.strip().split("=", 1)
                    team[gh.strip()] = discord.strip()
    return team

TEAM = load_team()

def get_latest_reviews(pr_number):
    reviews = github_api(f"repos/{REPO}/pulls/{pr_number}/reviews")
    latest = {}
    if reviews:
        for r in reviews:
            if r.get("state"):
                latest[r["user"]["login"]] = r["state"].upper()
    return latest

def has_label(pr, label_name):
    return any(label["name"] == label_name for label in pr.get("labels", []))

def handle_initial_pr():
    pr = github_api(f"repos/{REPO}/pulls/{PR_NUMBER}")
    if pr.get("draft"): return

    requested = [u["login"] for u in pr.get("requested_reviewers", [])]
    
    if requested:
        mentions = " ".join([f"<@{TEAM[u]}>" for u in requested if u in TEAM])
        discord_message(
            f"**PR #{PR_NUMBER} needs review**\n\n"
            f"**{pr['title']}**\nRequested: {mentions}\n\n<{pr['html_url']}>"
        )
    else:
        discord_message(
            f"**PR #{PR_NUMBER}\n"
            f"**{pr['title']}**\n@everyone\n\n<{pr['html_url']}>"
        )

def handle_review_submitted():
    review_state = os.environ.get("REVIEW_STATE", "").upper()
    reviewer = os.environ.get("REVIEWER", "")
    
    if review_state == "CHANGES_REQUESTED":
        pr = github_api(f"repos/{REPO}/pulls/{PR_NUMBER}")
        author = pr["user"]["login"]
        author_mention = f"<@{TEAM[author]}>" if author in TEAM else f"@{author}"
        
        discord_message(
            f"**Changes requested on PR #{PR_NUMBER}**\n"
            f"**{pr['title']}**\n{author_mention}, {reviewer} requested changes.\n\n<{pr['html_url']}>"
        )

def handle_synchronize():
    pr = github_api(f"repos/{REPO}/pulls/{PR_NUMBER}")
    if pr.get("draft"): return

    # Remove the reminder label so the clock resets for the new commit
    github_api(f"repos/{REPO}/issues/{PR_NUMBER}/labels/{REMINDER_LABEL}", method="DELETE")

    latest_reviews = get_latest_reviews(PR_NUMBER)
    re_reviewers = [u for u, state in latest_reviews.items() if state == "CHANGES_REQUESTED"]
    
    if re_reviewers:
        mentions = " ".join([f"<@{TEAM[u]}>" for u in re_reviewers if u in TEAM])
        discord_message(
            f"**{pr['title']}**\n{mentions} new changes were pushed. Please re-review.\n\n<{pr['html_url']}>"
        )

def handle_reminders():
    prs = github_api(f"repos/{REPO}/pulls?state=open")
    if not prs: return
    now = datetime.now(timezone.utc)
    
    for pr in prs:
        if pr.get("draft"): continue
        if has_label(pr, REMINDER_LABEL): continue
        
        updated_at = datetime.strptime(pr["updated_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        age_in_hours = (now - updated_at).total_seconds() / 3600
        
        if age_in_hours < REMINDER_DELAY_HOURS:
            continue
            
        author = pr["user"]["login"]
        latest_reviews = get_latest_reviews(pr["number"])
        done_users = [u for u, state in latest_reviews.items() if state in ("APPROVED", "COMMENTED")]
        
        if len(done_users) >= MIN_REVIEWS:
            continue
            
        slacking_members = [
            gh_user for gh_user in TEAM 
            if gh_user != author and gh_user not in done_users
        ]
        
        mentions = " ".join([f"<@{TEAM[u]}>" for u in slacking_members])
        needed = MIN_REVIEWS - len(done_users)
        
        if mentions:
            discord_message(
                f"**Reminder: PR #{pr['number']} still needs {needed} review(s)**\n\n"
                f"**{pr['title']}**\nWaiting on: {mentions}\n\n<{pr['html_url']}>"
            )
            
            # Tag the PR so we don't spam
            github_api(f"repos/{REPO}/issues/{pr['number']}/labels", method="POST", data={"labels": [REMINDER_LABEL]})

if __name__ == "__main__":
    if EVENT_NAME == "schedule" or EVENT_NAME == "workflow_dispatch":
        handle_reminders()
    elif EVENT_NAME in ("pull_request", "pull_request_target"):
        if EVENT_ACTION in ("opened", "ready_for_review"):
            handle_initial_pr()
        elif EVENT_ACTION == "synchronize":
            handle_synchronize()
    elif EVENT_NAME == "pull_request_review" and EVENT_ACTION == "submitted":
        handle_review_submitted()