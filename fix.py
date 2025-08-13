1) running the same code give same inputs and outupts, but created vs intermediate is changing --> therefore network changing:: context to be refined here. eample notice_of_default_fee
2) we need to look at context to define created and input vs parameter by language. to be refined. example term_plus_12
3) SAS adding variables looped (average_ead_i) vs python not - algin instructions there.
4) sometime it is adding data sets to variable - context to be refinde there

Master, excellent, let’s lock these down methodically. I’ll give you a crisp fix plan per point, with concrete rules and minimal code hooks you can drop into models/Data_Lineage.py so classifications and graphs stop “jumping”.

⸻

1) Deterministic “Created vs Intermediate” so the network stops changing

Symptom: same inputs and outputs, yet notice_of_default_fee flips between Created and Intermediate.
Root cause: the current _simple_ai_variable_extraction and downstream classification rely on LLM heuristics and order‑of‑parsing, so ties and edge cases wobble between buckets.

Deterministic rule stack (applied in this exact order)
	1.	Input if the name is a dataset column read from an external dataset and never appears on the left side of an assignment in code.
	2.	Created if it appears on the left side of an assignment or as an alias/derived expression in a SELECT list, and either
	•	the RHS contains any operator, function call, CASE/IF expression, or combine/concat, or
	•	it is a direct rename of an input variable.
	3.	Intermediate if it is Created but never written to an output dataset nor exported, nor referenced in a final SELECT/export step.
	4.	Tie-breaker: if a name is both read and later assigned, treat as Created.
	5.	Stability: always sort variable lists and de-duplicate using a canonical key (varname.lower(), first_definition_line).

Language-specific LHS and “derived” detection
	•	SAS: var = expression; in DATA step, PROC SQL SELECT ... AS var, IF/CASE indicates derivation.
	•	SQL: any AS var in the SELECT list, or computed tokens in expressions.
	•	Python (pandas): df['var'] = ... or df.assign(var=...) with non-trivial RHS.
	•	R (dplyr/base): mutate(var = ...), $var <-, DT[, var := ...].

Specific to notice_of_default_fee: it should be Created if it is calculated from fees, flags, or CASE logic. If later filtered out and not exported, it lands in Intermediate. The rule above makes that stable, determined by whether it surfaces in an output dataset.

Implementation hook: add a deterministic classifier layer that runs after extraction, before visualization:

# inside DataLineageAnalyzer
def classify_variables_deterministically(self, graph, read_cols, write_cols, assigns, final_select_cols):
    """
    graph: internal edges (var<-sources)
    read_cols: set of dataset columns observed in reads
    write_cols: set of dataset columns observed in writes/exports
    assigns: dict var -> first_assignment_line, rhs_tokens
    final_select_cols: vars referenced in last-mile outputs
    returns dict with keys: input_variables, created_variables, intermediate_variables, all_variables
    """
    # 1. Input
    input_vars = {c for c in read_cols if c not in assigns}
    # 2. Created
    created_vars = set()
    for v, meta in assigns.items():
        rhs = meta.get("rhs", "")
        is_derived = self._is_derived_expression(rhs)  # regex-based per language
        if is_derived or v in graph:  # rename/alias becomes derived too
            created_vars.add(v)
    # 3. Intermediate
    promoted_outputs = set(final_select_cols) | set(write_cols)
    intermediate = {v for v in created_vars if v not in promoted_outputs}
    # 4. Tie-breaker
    input_vars -= created_vars
    # 5. Canonicalize
    return self._canonicalize_buckets(input_vars, intermediate, created_vars)

This removes randomness and locks the network.

⸻

2) “Parameter” vs “Input” by language, and the term_plus_12 case

Goal: treat configuration and function arguments as Parameters, not Inputs; then classify computed features like term_plus_12 correctly as Created.

Parameter detection by language
	•	SAS: macro variables &X and %LET X=...; or %sysfunc(...) results are Parameters.
	•	Python: function arguments, constants defined at module scope, CONFIG = {...}, os.getenv() results are Parameters.
	•	R: function arguments, options, constants, Sys.getenv() are Parameters.
	•	SQL: bind variables :p_x, CTE parameters from application layer are Parameters.

Mapping into your current UI

Your manual-upload schema allows Input, Intermediate, Created, Output. To avoid breaking the UI:
	•	Track role = Parameter internally.
	•	Expose Parameter in the table as Variable Type = Input with a sub-role tag Parameter in a separate column (e.g., “Role”).
	•	Keep “true Inputs” as dataset columns only.

Concrete rule for term_plus_12:
If Term is a dataset column, then term_plus_12 = Term + 12 is Created.
If Term is a Parameter from config or macro, then term_plus_12 is also Created but sourced from a Parameter, not an Input.

Implementation hook: augment extraction to produce roles = {var: 'Parameter'|'DatasetColumn'|'Temp'} and pass this into the deterministic classifier. Add a small “Role” column to the lineage table and a legend in the viz.

⸻

3) SAS loop-created variables average_ead_i vs Python

Problem: SAS often creates families via arrays and DO loops, while the Python path may create a single vector or not expand at all.

Harmonised family handling
	•	Detect looped families and model them as variable families with a template key, for example average_ead_{i}.
	•	Expand into concrete variables only if the loop bounds are resolvable at parse time. Otherwise, keep the family as a single node with a badge family.
	•	In visualization, show a collapsed node average_ead_{i} with an optional “expand family” toggle.

Detection hints
	•	SAS:
	•	array avg_ead{n} avg_ead1-avg_eadn;
	•	avg_ead{i} = ...; do i=1 to n; ...; end;
	•	Python (pandas/numpy):
	•	for i in range(n): df[f'average_ead_{i}'] = ...
	•	df.filter(regex='^average_ead_\\d+$') implies a family
	•	df['average_ead'] = ... vector, not a family, keep as single var.

Instruction alignment: when SAS produces average_ead_1 ... _N, but Python produces one column, treat the Python one as Created singular, and the SAS set as a family. The rules make cross‑language graphs consistent and comprehensible.

Implementation hook: return an additional structure:

families = {
  'average_ead_{i}': {
      'members': ['average_ead_1','average_ead_2', ...] or None,
      'language': 'sas',
      'resolved': True/False
  }
}

Visualizer shows a single node when resolved=False, or a collapsible cluster when True.

⸻

4) “Dataset added to variable” mix-ups

Symptom: dataset names are being attached as if they were variables, producing noisy edges.

Robust dataset vs variable separation

Build and maintain three disjoint dictionaries as you parse:
	•	Datasets read: from SET/MERGE (SAS), FROM tables (SQL), pd.read_* sources and assigned DataFrame symbols (Python), read.csv/fread (R).
	•	Datasets written: CREATE TABLE ... AS (SQL), DATA out; set ... (SAS), to_csv/to_parquet targets (Python/R).
	•	Variables (columns): only column selectors, LHS of column assignment, SELECT list aliases, mutate/assign columns.

Key guards
	•	Never create a dataset -> variable edge unless the variable is explicitly selected from that dataset or created into that dataset.
	•	In pandas, distinguish the frame symbol (df, stg_ecl) from its columns. df is a dataset node, df['x'] is a variable node.
	•	In SAS DATA steps, the DATA out; SET in; lines set the dataset context. Variables named in KEEP=/DROP= or implicit pass-through belong to in. Assignments inside the step create Created variables into out unless dropped.

Implementation hook: maintain a current dataset context stack per language block, so columns seen within a block are mapped to the right dataset, and dataset tokens never end up in the variable bag.

⸻

What I recommend you implement first

Since your pain #1 drives the flickering graph, start by inserting the deterministic classifier. It is self-contained and will stabilise everything:
	1.	Parse → collect
	•	read_cols, write_cols, final_select_cols, assigns[var] = {'line': n, 'rhs': '...'}
	2.	Run classify_variables_deterministically(...) as above.
	3.	Store role metadata to distinguish Parameters.
	4.	Feed these stable buckets into your existing viz and table.

Once that lands, add the Parameter role (#2), then family handling (#3), and finally the dataset guards (#4). Each step is incremental and does not break the UI.

If you want, Master, I can provide a compact patch for DataLineageAnalyzer with:
	•	_is_derived_expression regexes per language,
	•	classify_variables_deterministically,
	•	a tiny change in the visualizer to show a “Role” column and collapse families.