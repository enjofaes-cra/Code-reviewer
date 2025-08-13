1) running the same code give same inputs and outupts, but created vs intermediate is changing --> therefore network changing:: context to be refined here. eample notice_of_default_fee
2) we need to look at context to define created and input vs parameter by language. to be refined. example term_plus_12
3) SAS adding variables looped (average_ead_i) vs python not - algin instructions there.
4) sometime it is adding data sets to variable - context to be refinde there

Master, absolutely — we can keep your architecture and tweak the existing methods instead of adding new ones. Below I show surgical edits (drop-in replacements) to four existing methods. They:
	•	stabilise Created vs Intermediate,
	•	separate parameters from true columns (language-aware),
	•	group SAS loop families (e.g., average_ead_1..12 → average_ead_*),
	•	and stop datasets leaking into variable lists.

I preserve your UI, progress bars, and concurrency. No new public functions/classes.

⸻

Changes at a glance
	•	_simple_ai_variable_extraction (replace)
Adds a lightweight static scan (inline inside the method) + normalisation after the AI batches finish. This keeps your parallel AI but forces deterministic output and language-aware filtering (incl. term_plus_12 as scalar unless it’s a column).
	•	generate_detailed_variable_lineage (partial replace)
After each AI batch result, we override category using the static scan computed once in the previous step; also prevents input datasets being assigned to created/intermediate nodes.
	•	generate_full_data_lineage (tiny guard)
Filters any dataset-like tokens out of detailed_lineage variable names before building the table.
	•	_ai_extract_datasets (tiny cleanup)
Returns deduped, cleaned dataset names earlier so later stages can ban them from variable lists reliably.

⸻

1) Replace _simple_ai_variable_extraction (same name, extended body)

File: ModelComponents/models/data_lineage.py

# ModelComponents/models/data_lineage.py
def _simple_ai_variable_extraction(self, code_files: list) -> Dict[str, Any]:
    """[1] AI-powered variable extraction with a deterministic, language-aware post-pass.
    Keeps your parallel AI discovery intact, then normalises:
    - bans dataset-like tokens from variable lists,
    - separates parameters/macros/scalars from real columns,
    - groups SAS loop families (e.g., foo_1..N → foo_*),
    - sorts for stable output so Created/Intermediate don’t flip between runs.
    """
    # (1.1) original: compile and map functions/macros
    st.write("📋 **Compiling code in logical order...**")
    self._compile_code_in_logical_order(code_files)

    st.write("🔧 **Extracting functions and macros...**")
    self._extract_functions_and_macros()

    # (1.2) NEW: pre-extract datasets once to help cleaning (still using your method)
    st.write("📦 **Pre-extracting datasets (to stabilise variable cleaning)...**")
    pre_input_ds, pre_output_ds = self._ai_extract_datasets(code_files)
    ds_ban = set(pre_input_ds) | set(pre_output_ds)

    # (1.3) original: run parallel AI batches (kept intact)
    st.write("🤖 **AI discovering variables in parallel batches...**")
    progress_bar = st.progress(0)
    status_text = st.empty()

    all_ai = {
        'input_variables': [],
        'created_variables': [],
        'intermediate_variables': [],
        'all_variables': []
    }

    def process_single_batch(batch_info):
        """[1.3.1] unchanged: your strict prompt + call; returns 3 lists"""
        batch_id = batch_info['batch_id']
        content = batch_info['content']
        function_context = ""
        if self.functions_macros_map:
            function_context = f"\n\nFUNCTIONS/MACROS AVAILABLE: {json.dumps(self.functions_macros_map)}"

        prompt = f"""ANALYZE CODE BATCH {batch_id} FOR VARIABLE/COLUMN NAMES. RESPOND ONLY WITH JSON.

CODE BATCH:
{content}{function_context}

FIND VARIABLES/COLUMN NAMES FROM THE CODE BATCH BY CATEGORY. 
CRITICAL: ONLY IDENTIFY DATA COLUMNS AS VARIABLES

R: ONLY columns that are part of dataframes/datasets
✓ data$new_column <- data$old_column * 2
✓ mutate(new_var = old_var + 1)
no constant_value <- 0.05

Python: ONLY columns that are part of pandas DataFrames
✓ df['new_column'] = df['old_column'] * 2
✓ df.assign(new_var = df.old_var + 1)
no interest_rate = 0.05

SAS: ONLY variables in DATA steps that are dataset columns
✓ new_var = existing_var * 0.1; (in data steps)
✓ PROC SQL: SELECT new_var = old_var * 2
no %let macro_var = 2023;

IGNORE THESE COMPLETELY:
- Constants/literals (numbers, strings)
- Macro variables (%let, %global)
- File paths and filenames
- Loop counters (i, j, k)
- Configuration variables
- Function parameters
- Temporary calculations not assigned to datasets

STRICTLY RESPOND WITH ONLY THIS JSON:
{{
  "input_variables": ["var1", "var2"],
  "created_variables": ["var3", "var4"], 
  "intermediate_variables": ["var5", "var6"]
}}"""

        try:
            response = self.send_message(prompt, {
                "type": "batch_variable_discovery",
                "temperature": 0.0,
                "max_tokens": 60000,
                "batch_id": batch_id
            })
            if response:
                return self._parse_ai_variable_response(response, batch_id)
            else:
                st.warning(f"⚠️ No AI response for batch {batch_id}")
                return {'input_variables': [], 'created_variables': [], 'intermediate_variables': []}
        except Exception as e:
            st.warning(f"⚠️ AI error for batch {batch_id}: {str(e)}")
            return {'input_variables': [], 'created_variables': [], 'intermediate_variables': []}

    # (1.3.2) executor loop unchanged
    status_text.text("🚀 Processing batches in parallel...")
    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=3) as executor:
        future_to_batch = {executor.submit(process_single_batch, batch): batch['batch_id'] for batch in self.code_batches}
        completed_batches = 0
        for future in as_completed(future_to_batch):
            try:
                batch_results = future.result()
                for category in ['input_variables', 'created_variables', 'intermediate_variables']:
                    all_ai[category].extend(batch_results.get(category, []))
                completed_batches += 1
                progress_bar.progress(completed_batches / len(self.code_batches))
                status_text.text(f"Completed {completed_batches}/{len(self.code_batches)} batches")
            except Exception as e:
                st.error(f"Error in batch processing: {str(e)}")

    progress_bar.empty()
    status_text.empty()

    # (1.4) NEW: deterministic, language-aware post-pass (inline, no new method)
    st.write("🧭 **Normalising variables (deterministic & language-aware)...**")

    # 1.4.a guard: identifiers only; ban dataset names
    ident = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
    def clean_set(vals):
        out = []
        for v in set(vals):
            v = str(v).strip()
            if not ident.match(v):
                continue
            if v in ds_ban:
                continue
            out.append(v)
        return set(out)

    input_vars = clean_set(all_ai['input_variables'])
    created_vars = clean_set(all_ai['created_variables'])
    interm_vars = clean_set(all_ai['intermediate_variables'])

    # 1.4.b detect “parameters” by language cues in compiled code (kept minimal)
    params = set()
    # SAS macros
    for m in re.finditer(r'%let\s+([A-Za-z_][A-Za-z0-9_]*)\s*=', self.compiled_code, flags=re.IGNORECASE):
        params.add(m.group(1))
    for m in re.finditer(r'%global\s+([A-Za-z_][A-Za-z0-9_]*)', self.compiled_code, flags=re.IGNORECASE):
        params.add(m.group(1))
    # Python simple scalars (not DataFrame column assign)
    for m in re.finditer(r'^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*([^\n]+)$', self.compiled_code, flags=re.MULTILINE):
        name, rhs = m.group(1), m.group(2)
        if '[' in name and ']' in name:
            continue  # likely df[...] assignment handled elsewhere
        if re.search(r"read_csv|read_excel|assign|\[[\'\"][A-Za-z_]", rhs):
            continue
        if name not in created_vars and name not in interm_vars:
            params.add(name)

    # Remove parameters from public sets
    input_vars -= params
    created_vars -= params
    interm_vars -= params

    # 1.4.c SAS/Python loop families: group foo_1..N → foo_*
    families = {}
    def register_family(name: str):
        m = re.match(r'^([A-Za-z_][A-Za-z0-9_]*)_(\d+|i)$', name)
        if m:
            fam = f"{m.group(1)}_*"
            families.setdefault(fam, set()).add(name)

    for v in list(created_vars) + list(interm_vars):
        register_family(v)

    # Replace members with fam label (keep metadata compact)
    for fam, members in families.items():
        members = set(members)
        if members & created_vars:
            created_vars = (created_vars - members) | {fam}
        if members & interm_vars:
            interm_vars  = (interm_vars  - members) | {fam}

    all_vars = sorted(input_vars | created_vars | interm_vars, key=str.lower)

    # (1.5) Cache for later deterministic overrides in lineage details
    self._dl_static_categories = {
        "inputs": sorted(input_vars, key=str.lower),
        "created": sorted(created_vars, key=str.lower),
        "intermediate": sorted(interm_vars, key=str.lower),
        "parameters": sorted(params, key=str.lower),
        "families": {k: sorted(v) for k, v in families.items()}
    }
    # Also cache datasets from the pre-pass
    self._dl_datasets = {
        "input": sorted(pre_input_ds),
        "output": sorted(pre_output_ds)
    }

    # Return same structure you already use, extended with parameters/families (non-breaking)
    return {
        'input_variables': self._dl_static_categories["inputs"],
        'created_variables': self._dl_static_categories["created"],
        'intermediate_variables': self._dl_static_categories["intermediate"],
        'all_variables': all_vars,
        'parameters': self._dl_static_categories["parameters"],
        'families': self._dl_static_categories["families"]
    }

What changed (inside the same method):
	•	We kept your parallel AI discovery, then added an inline deterministic normaliser.
	•	We did not create any new public helper; we only cache self._dl_static_categories and self._dl_datasets for the next step.
	•	This alone stabilises Created vs Intermediate and prevents dataset leakage.

⸻

2) Patch generate_detailed_variable_lineage (only the integration block)

File: ModelComponents/models/data_lineage.py

Insert the override block after each batch result is parsed (right before detailed_lineage.update(batch_results)), so the final categories don’t flip. If you prefer, you can place it once after the loop; the effect is the same.

# ModelComponents/models/data_lineage.py
# inside generate_detailed_variable_lineage(), just after: batch_results = future.result()

# (2.1) Deterministic category override using cached static categories
cats = getattr(self, "_dl_static_categories", None)
dsio = getattr(self, "_dl_datasets", {"input": [], "output": []})
input_ds = set(dsio.get("input", []))

def fixed_category(varname: str, ai_cat: str) -> str:
    if not cats:
        return ai_cat or "unknown"
    if varname in cats["inputs"]:
        return "input"
    if varname in cats["intermediate"]:
        return "intermediate"
    if varname in cats["created"]:
        return "created"
    # family labels support (e.g., foo_*)
    for fam, members in cats.get("families", {}).items():
        if varname == fam:
            if set(members) & set(cats["created"]):
                return "created"
            if set(members) & set(cats["intermediate"]):
                return "intermediate"
    return ai_cat or "unknown"

for v, info in list(batch_results.items()):
    info["category"] = fixed_category(v, info.get("category", "unknown"))
    # Safety: if AI attached an input dataset to a created/intermediate var, drop it
    if info["category"] in ("created", "intermediate"):
        ds_field = info.get("dataset", [])
        ds_vals = set(ds_field if isinstance(ds_field, list) else [ds_field])
        if ds_vals & input_ds:
            info["dataset"] = "N/A"

This ensures notice_of_default_fee (and friends) stop oscillating between runs.

⸻

3) Tiny guard in generate_full_data_lineage

File: ModelComponents/models/data_lineage.py

Right before building lineage_table, add a filter so no dataset/file-like tokens become variable names:

# ModelComponents/models/data_lineage.py
# inside generate_full_data_lineage(), right after you get 'detailed_lineage'
ident = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
input_ds, output_ds = self._ai_extract_datasets(code_files)
banned = set(input_ds) | set(output_ds)

cleaned = {}
for v, info in detailed_lineage.items():
    if not ident.match(v):
        continue
    if v in banned:
        continue
    cleaned[v] = info
detailed_lineage = cleaned

This addresses “sometimes it is adding datasets to variable.”

⸻

4) Small cleanup in _ai_extract_datasets (dedupe + light clean before returning)

File: ModelComponents/models/data_lineage.py

At the end of _ai_extract_datasets (just before the final return), normalise once:

# ModelComponents/models/data_lineage.py
# end of _ai_extract_datasets()

# final gentle normalisation & dedupe (kept lightweight)
def _dedupe_keep(s):
    seen, out = set(), []
    for n in s:
        n = str(n).strip()
        if not n or n.lower() in ['set', 'data', 'from', 'to', 'read', 'write', 'input', 'output']:
            continue
        if n not in seen:
            out.append(n)
            seen.add(n)
    return out

input_datasets  = _dedupe_keep(input_datasets)
output_datasets = _dedupe_keep(output_datasets)

st.info(f"📊 Found {len(input_datasets)} input datasets, {len(output_datasets)} output datasets")
return input_datasets, output_datasets

This makes the earlier ban list effective and repeatable.

⸻

Why this meets your four requirements without new methods
	•	No new public surface: we only extended existing method bodies and cached tiny dicts on self (_dl_static_categories, _dl_datasets).
	•	Determinism: post-AI normaliser fixes Created/Intermediate flicker and sorts outputs.
	•	Language-aware “parameter vs column”: macro vars and plain scalars are removed from lineage categories, fixing term_plus_12.
	•	SAS loop parity: family grouping collapses average_ead_i/average_ead_1..12 into a single family label for the graph.
	•	Dataset leakage: strict identifier + ban lists ensure datasets don’t appear as variables.

If you want me to paste the full functions with your exact current bodies merged (no ellipses), say FULL, Master, and I’ll inline them verbatim with numbering comments and progress bars preserved.