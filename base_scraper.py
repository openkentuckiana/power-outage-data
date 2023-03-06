import base64
import json

from requests import Session


class GithubContents:
    # https://github.com/simonw/github-contents/blob/main/github_contents.py
    class NotFound(Exception):
        pass

    class UnknownError(Exception):
        pass

    def __init__(self, owner, repo, token, branch="main"):
        self.owner = owner
        self.repo = repo
        self.token = token
        self.session = Session()
        self.branch = branch

    def base_url(self):
        return "https://api.github.com/repos/{}/{}".format(self.owner, self.repo)

    def headers(self):
        return {"Authorization": "token {}".format(self.token)}

    def read(self, filepath):
        "Returns (file_contents_in_bytes, sha1)"
        # Try reading using content API
        content_url = "{}/contents/{}".format(self.base_url(), filepath)
        response = self.session.get(content_url, headers=self.headers())
        if response.status_code == 200:
            data = response.json()
            if not data.get("content") and data.get("download_url"):
                return self.session.get(data["download_url"], headers=self.headers()).text, data["sha"]
            return base64.b64decode(data["content"]), data["sha"]
        elif response.status_code == 404:
            raise self.NotFound(filepath)
        elif response.status_code == 403:
            # It's probably too large
            if response.json()["errors"][0]["code"] != "too_large":
                raise self.UnknownError(response.content)
            else:
                return self.read_large(filepath)
        else:
            raise self.UnknownError(response.content)

    def read_large(self, filepath):
        "Returns (file_contents_in_bytes, sha1)"
        default_tree = self.session.get(
            self.base_url() + "/git/trees/{}?recursive=1".format(self.branch),
            headers=self.headers(),
        ).json()
        try:
            tree_entry = [t for t in default_tree["tree"] if t["path"] == filepath][0]
        except IndexError:
            raise self.NotFound(filepath)
        data = self.session.get(tree_entry["url"], headers=self.headers()).json()
        return base64.b64decode(data["content"]), data["sha"]

    def write(
            self, filepath, content_bytes, sha=None, commit_message="", committer=None
    ):
        if not isinstance(content_bytes, bytes):
            raise TypeError("content_bytes must be a bytestring")
        github_url = "{}/contents/{}".format(self.base_url(), filepath)
        payload = {
            "path": filepath,
            "content": base64.b64encode(content_bytes).decode("latin1"),
            "message": commit_message,
        }
        if sha:
            payload["sha"] = sha
        if committer:
            payload["committer"] = committer

        response = self.session.put(github_url, json=payload, headers=self.headers())
        if (
                response.status_code == 403
                and response.json()["errors"][0]["code"] == "too_large"
        ):
            return self.write_large(filepath, content_bytes, commit_message, committer)
        elif (
                sha is None
                and response.status_code == 422
                and "sha" in response.json().get("message", "")
        ):
            # Missing sha - we need to figure out the sha and try again
            _, old_sha = self.read(filepath)
            return self.write(
                filepath,
                content_bytes,
                sha=old_sha,
                commit_message=commit_message,
                committer=committer,
            )
        elif response.status_code in (201, 200):
            updated = response.json()
            return updated["content"]["sha"], updated["commit"]["sha"]
        else:
            raise self.UnknownError(
                str(response.status_code) + ":" + repr(response.content)
            )

    def write_large(self, filepath, content_bytes, commit_message="", committer=None):
        if not isinstance(content_bytes, bytes):
            raise TypeError("content_bytes must be a bytestring")
        # Create a new blob with the file contents
        created_blob = self.session.post(
            self.base_url() + "/git/blobs",
            json={
                "encoding": "base64",
                "content": base64.b64encode(content_bytes).decode("latin1"),
            },
            headers=self.headers(),
        ).json()
        # Retrieve default tree sha
        default_branch_sha = self.session.get(
            self.base_url() + "/git/trees/{}?recursive=1".format(self.branch),
            headers=self.headers(),
        ).json()["sha"]
        # Construct a new tree
        created_tree = self.session.post(
            self.base_url() + "/git/trees",
            json={
                "base_tree": default_branch_sha,
                "tree": [
                    {
                        "mode": "100644",  # file (blob),
                        "path": filepath,
                        "type": "blob",
                        "sha": created_blob["sha"],
                    }
                ],
            },
            headers=self.headers(),
        ).json()
        # Create a commit which references the new tree
        payload = {
            "message": commit_message,
            "parents": [default_branch_sha],
            "tree": created_tree["sha"],
        }
        if committer:
            payload["committer"] = committer
        created_commit = self.session.post(
            self.base_url() + "/git/commits", json=payload, headers=self.headers()
        ).json()
        # Move HEAD reference on master to the new commit
        self.session.patch(
            self.base_url() + "/git/refs/heads/{}".format(self.branch),
            json={"sha": created_commit["sha"]},
            headers=self.headers(),
        ).json()
        return created_blob["sha"], created_commit["sha"]

    def branch_exists(self):
        assert self.branch
        return (
                self.session.get(
                    self.base_url() + "/git/refs/heads/{}".format(self.branch),
                    headers=self.headers(),
                ).status_code
                == 200
        )

class Scraper:
    owner = None
    repo = None
    filepath = None
    committer = {"name": "outage-scrapers", "email": "none@example.com"}
    test_mode = False

    def __init__(self, github_token):
        self.last_data = None
        self.last_sha = None
        self.github_token = github_token

    def create_message(self, new_data):
        return "Created {}".format(self.filepath)

    def update_message(self, old_data, new_data):
        return "Updated {}".format(self.filepath)

    def fetch_data(self):
        return []

    def scrape_and_store(self):
        # We need to store the data
        github = GithubContents(self.owner, self.repo, self.github_token)
        if not self.last_data or not self.last_sha:
            # Check and see if it exists yet
            try:
                content, sha = github.read(self.filepath)
                self.last_data = json.loads(content)
                self.last_sha = sha
            except GithubContents.NotFound:
                self.last_data = None
                self.last_sha = None

        data = self.fetch_data()
        if data is None:
            print("{}; Data was None".format(self.filepath))
            return

        if self.test_mode and not self.github_token:
            print(json.dumps(data, indent=2))
            return

        if self.last_data == data:
            print("%s: Nothing changed" % self.filepath)
            return

        if self.last_sha:
            print("Updating {}".format(self.filepath))
            message = self.update_message(self.last_data, data)
        else:
            print("Creating {}".format(self.filepath))
            message = self.create_message(data)

        if self.test_mode:
            print(message)
            print()
            print(json.dumps(data, indent=2))
            return

        content_sha, commit_sha = github.write(
            filepath=self.filepath,
            content_bytes=json.dumps(data, indent=2).encode("utf8"),
            sha=self.last_sha,
            commit_message=message,
            committer=self.committer,
        )

        self.last_sha = content_sha
        self.last_data = data
        print("https://github.com/{}/{}/commit/{}".format(self.owner, self.repo, commit_sha))


class DeltaScraper(Scraper):
    """
    The fetch_data() method should return a list of dicts. Each dict
    should have a key that can be used to identify the row in that dict.

    Then you define a display_record(record) method that returns a string.
    """

    record_key = None
    show_changes = False
    noun = "record"
    plural = None
    source_url = None

    @property
    def display_name(self):
        return self.filepath.replace(".json", "")

    @property
    def noun_plural(self):
        return self.plural or (self.noun + "s")

    def display_record(self, record):
        pairs = []
        for key, value in record.items():
            pairs.append("{} = {}".format(key, value))
        return "\n".join(pairs)

    def display_changes(self, old_record, new_record):
        changes = []
        for key in old_record:
            prev = old_record[key]
            next = new_record.get(key)
            if prev != next:
                changes.append("{}: {} => {}".format(key, prev, next))
        return "\n".join(changes)

    def create_message(self, new_records):
        return self.update_message([], new_records, "Created")

    def update_message(self, old_records, new_records, verb="Updated"):
        previous_ids = [record[self.record_key] for record in old_records]
        current_ids = [record[self.record_key] for record in new_records]
        added_ids = [id for id in current_ids if id not in previous_ids]
        removed_ids = [id for id in previous_ids if id not in current_ids]

        message_blocks = []
        if added_ids:
            messages = []
            messages.append("{} new {}:".format(len(added_ids), self.noun if len(added_ids) == 1 else self.noun_plural,))
            for id in added_ids:
                record = [r for r in new_records if r[self.record_key] == id][0]
                messages.append(self.display_record(record))
            message_blocks.append(messages)

        if removed_ids:
            messages = []
            messages.append(
                "{} {} removed:".format(len(removed_ids), self.noun if len(removed_ids) == 1 else self.noun_plural,)
            )
            for id in removed_ids:
                record = [r for r in old_records if r[self.record_key] == id][0]
                messages.append(self.display_record(record))
            message_blocks.append(messages)

        # Add useful rendering of CHANGED records as well
        changed_records = []
        for new_record in new_records:
            try:
                old_record = [r for r in old_records if r[self.record_key] == new_record[self.record_key]][0]
            except IndexError:
                continue
            if json.dumps(old_record, sort_keys=True) != json.dumps(new_record, sort_keys=True):
                changed_records.append((old_record, new_record))

        if self.show_changes and changed_records:
            messages = []
            messages.append(
                "{} {} changed:".format(len(removed_ids), self.noun if len(removed_ids) == 1 else self.noun_plural,)
            )
            for old_record, new_record in changed_records:
                messages.append(self.display_changes(old_record, new_record))
            message_blocks.append(messages)

        blocks = []
        for message_block in message_blocks:
            block = "\n".join(message_block)
            blocks.append(block.strip())

        if self.source_url:
            blocks.append("Detected on {}".format(self.source_url))

        body = "\n\n".join(blocks)

        summary = []
        if added_ids:
            summary.append("{} {} added".format(len(added_ids), self.noun if len(added_ids) == 1 else self.noun_plural,))
        if removed_ids:
            summary.append("{} {} removed".format(len(removed_ids), self.noun if len(removed_ids) == 1 else self.noun_plural,))
        if changed_records:
            summary.append(
                "{} {} changed".format(len(changed_records), self.noun if len(changed_records) == 1 else self.noun_plural,)
            )
        if summary:
            summary_text = self.display_name + ": " + (", ".join(summary))
        else:
            summary_text = "{} {}".format(verb, self.display_name)
        return "{}\n\n{}".format(summary_text, body)
