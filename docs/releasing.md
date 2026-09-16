# How to Make a Release

A release is just a **tag**. You pick a number, push the tag, and a robot does the rest.

You do not edit any files. The version number comes from the tag.

## Step 1 — Pick a number

| What changed        | New version | Example            |
| ------------------- | ----------- | ------------------ |
| Small fix           | `vX.Y.(Z+1)` | `v1.1.0` → `v1.1.1` |
| New feature         | `vX.(Y+1).0` | `v1.1.0` → `v1.2.0` |
| Big / breaking change | `v(X+1).0.0` | `v1.1.0` → `v2.0.0` |

Always start the tag with `v`. Use three numbers separated by dots.

### Pre-release (test) versions

For a release candidate or a test, add a suffix: `v1.2.0-rc1`, `v1.2.0-beta1`. These are
marked as **pre-releases** on GitHub and never become the "Latest" release. Use one when
you want to try the process without publishing a real version.

## Step 2 — Type two commands

In the project folder, on the `main` branch, with your work already pushed:

```bash
git tag -a v1.2.0 -m "v1.2.0"
git push origin v1.2.0
```

Change `v1.2.0` to the number you picked. That's it.

## Step 3 — Wait about two minutes

1. Open the repository on GitHub.
2. Click the **Actions** tab.
3. Find the run named **Release**. A green tick ✅ means it worked.

When it is done, open the **Releases** page. Your new release has three files:

- `hkex_filing_scraper-<version>-py3-none-any.whl`
- `hkex_filing_scraper-<version>.tar.gz`
- `SHA256SUMS`

## Step 4 — Anyone can now install it

```bash
pip install "git+https://github.com/simonplmak-cloud/hkex-filing-scraper@v1.2.0"
```

Or download the `.whl` file from the Releases page and install that.

## If something goes wrong

The **Actions** tab shows a red ❌. Click the failed step to read the error, fix it on
`main`, and push. Then remove the release and the tag, and try again:

```bash
# Deletes the GitHub Release AND its tag in one command:
gh release delete v1.2.0 --yes --cleanup-tag

# If you also created the tag locally:
git tag -d v1.2.0
# fix the problem, push to main, then repeat Step 2
```

> Deleting a tag on its own does **not** delete its GitHub Release — use
> `gh release delete <tag> --yes --cleanup-tag` (or the "Delete" button on the
> Releases page) to remove both.

If the robot is badly broken and you need the files **right now**, there is a manual
escape hatch: see [Release automation → Manual fallback](release-automation.md#manual-fallback).

## Want to understand how it works?

Read [docs/release-automation.md](release-automation.md). It explains every step, what can
break, and how to fix it — written so a future maintainer can repair it years later.
