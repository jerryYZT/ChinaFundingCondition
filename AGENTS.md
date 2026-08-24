# ChinaFundingCondition

A Python data-analysis project. `ChinaTreasury.ipynb` is a Jupyter/Colab notebook that uses the
[`akshare`](https://akshare.akfamily.xyz/) library with `pandas` to query Chinese treasury/bond
market data.

## Cursor Cloud specific instructions

- Python packages install to `~/.local/bin` (user install), which is added to `PATH` via `~/.bashrc`.
  New non-login shells should already have it; if `jupyter` is "not found", run
  `export PATH="$HOME/.local/bin:$PATH"`.
- Dependencies are in `requirements.txt`. There is no lint or automated test suite in this repo; the
  "app" is the notebook itself.
- Run JupyterLab with: `jupyter lab --no-browser --ip=0.0.0.0 --port=8888 --ServerApp.token="" --ServerApp.password=""`
  (open `http://localhost:8888/lab`).
- Execute the notebook headlessly with:
  `jupyter nbconvert --to notebook --execute --allow-errors ChinaTreasury.ipynb --output /tmp/out.ipynb`
- `ipywidgets` is required: `akshare` renders `tqdm` progress bars, and without `ipywidgets` some
  `akshare` calls raise `ImportError` inside the JupyterLab kernel (headless CLI Python does not need it,
  but the notebook UI does).
- `akshare` fetches live data from external Chinese financial endpoints, so cells need network egress and
  can take several seconds to tens of seconds. `ak.bond_zh_us_rate()` is a reliable smoke test that
  returns thousands of rows of China/US treasury yields.
- Known pre-existing issue (NOT an environment problem): `ChinaTreasury.ipynb` cell 2 calls
  `ak.bond_info_cm_query()`, which raises `KeyError: 'bondRtngShrt'` due to an upstream akshare/API
  change. This error is already saved in the committed notebook output. Do not "fix" it as part of
  environment setup. `ak.bond_info_cm_query(symbol="债券类型")` and
  `ak.bond_info_cm(bond_type="国债")` still work; only the default symbol `"评级等级"` is broken.
- `treasury_ib_flow.py` pulls **interbank-listed** China treasury issuance and maturity for one date:
  `python treasury_ib_flow.py 2026-08-21`. Issuance comes from cninfo (filter `交易市场` containing
  银行间, so the same book-entry bond is not triple-counted across SSE/SZSE/interbank). Maturity
  comes from chinamoney `BondMarketInfoList2` + `BondDetailInfo`. First maturity run writes
  `.cache/ib_treasury_details.csv` (gitignored, a few minutes); later runs reuse it unless
  `--refresh-cache`. Use `--issue-on 缴款日` if matching cash-settlement date instead of auction date.
