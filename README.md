# DataMindning

## Data Access (Google Drive)

Dataset is stored on Google Drive due to GitHub file size limits.

- Download data here: **PASTE_YOUR_GOOGLE_DRIVE_LINK_HERE**

### Files provided in the Drive folder
- `raw_samples_30000_final_20260309.jsonl`
- `cleaned_data.jsonl`
- `chroma_db/` (optional local vector database)

## Quick Start

1. Clone this repository.
2. Download the dataset from Google Drive link above.
3. Put files in the project root (same level as scripts):
   - `raw_samples_30000_final_20260309.jsonl`
   - `cleaned_data.jsonl`
   - `chroma_db/` (if shared)
4. Run scripts as needed:
   - `python preprocess.py`
   - `python build_vector_db.py`
   - `python query_vector_db.py`

## Notes
- Large data files are intentionally excluded from Git tracking.
- If the Drive link is private, request access from repository owner.
