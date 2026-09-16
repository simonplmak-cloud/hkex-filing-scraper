# Legal, Terms of Use, and Data

This project is a **research and data-engineering tool**. It is not affiliated with, endorsed
by, or sponsored by Hong Kong Exchanges and Clearing Limited (HKEX).

## What this project is

- Software that queries a publicly reachable, **undocumented** HKEx JSON endpoint and stores
  the results in a database **you** control.
- Distributed under the MIT licence (`LICENSE`).

## What this project is not

- It does **not** ship, host, or redistribute any HKEx data. The repository contains no
  filings, no extracted text, and no datasets — only code and tests.
- It does not provide legal advice or a data licence.

## Your responsibilities

1. **Terms of Use.** Access to HKEx websites and services is governed by the HKEX Terms of
   Use. In particular, automated collection and **commercial redistribution** of HKEx data may
   require a licensed HKEx feed or written permission. Review the current terms before
   commercial use: <https://www.hkex.com.hk/Global/Exchange/Terms-of-Use>.
2. **Rate limiting.** Be a good citizen: keep concurrency modest, use the built-in batching,
   and do not hammer the endpoint. The scraper defaults are chosen to be polite
   (`MAX_DOWNLOAD_WORKERS`, monthly chunking, retries).
3. **Personal data & sensitive content.** Filings can contain personal data (e.g. director
   names). If you process them, comply with the laws that apply to you (for example, the
   Hong Kong Personal Data (Privacy) Ordinance and the GDPR where relevant).
4. **Attribution.** HKEx is the source of the underlying filings. Do not present the data as
   your own, and retain provenance (`source = 'HKEx'`, `documentUrl`, `filingId`).

## Licence of this software

MIT — see [LICENSE](../LICENSE). The MIT licence covers **this code only**, not any HKEx data
you collect with it.

## Contact

Questions about licensing this project's code: open an issue. Questions about HKEx data
licensing: contact HKEX directly.
