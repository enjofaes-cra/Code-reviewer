1) running the same code give same inputs and outupts, but created vs intermediate is changing --> therefore network changing:: context to be refined here. eample notice_of_default_fee
2) we need to look at context to define created and input vs parameter by language. to be refined. example term_plus_12
3) SAS adding variables looped (average_ead_i) vs python not - algin instructions there.
4) sometime it is adding data sets to variable - context to be refinde there
Master, perfect clarification. If lineage is code-only with no input datasets loaded, we must infer everything from static code signals. Below is a refined, deterministic rule‑set and implementation plan for each of your four points that relies solely on parsing the code, not on actual data files.

I will keep this precise and actionable.

⸻

1) Stable “Created vs Intermediate” from code only

Signals we can extract without data
	•	Assignments to columns/variables with first definition line and RHS text.
	•	SAS: target = expr;, PROC SQL: ... AS target
	•	Python: df['target'] = expr, df.assign(target=expr)
	•	R: mutate(target = expr), DT[, target := expr], $target <- expr
	•	SQL: SELECT expr AS target
	•	Usages of variables in later expressions.
	•	Sinks that imply “survives to output”: explicit writes or final selections:
	•	SAS: DATA out; SET ...; then anything not dropped at end of step; KEEP=, DROP=, OUTPUT;
	•	Python: to_csv, to_parquet, to_sql, return df at end of a function, or df[['col', ...]] immediately before a write
	•	R: write.csv, fwrite, arrow::write_*, function return value
	•	SQL: CREATE TABLE/VIEW, INSERT INTO, or the outermost SELECT list

Deterministic classification (code-only)

Apply in this order:
	1.	Candidate Created: any name assigned from an expression or alias in a SELECT.
	2.	Survives / Promoted: if the variable appears in a sink (see above), mark is_promoted = True.
	3.	Intermediate: Created and not promoted by any sink.
	4.	Input (code-only approximation): column/variable names referenced but never assigned in this codebase.
	•	Example: existing_balance used on RHS but nowhere on LHS is an Input.
	5.	Tie-breaker: if a name is both read and later assigned, it is Created (not Input).
	6.	Stability: canonical key (lower(name), first_def_line) and sort deterministically.

Why this fixes flicker: we eliminate AI heuristics for bucket choice. Whether notice_of_default_fee is Created or Intermediate depends solely on whether we see it flow into a sink in the same codebase. If not seen, it is Intermediate, deterministically.

⸻

2) Distinguish Parameter vs Input from code context and language

We will not rely on datasets, we infer Parameters from code patterns. Then “Input” becomes “columns/vars used but never defined here”.

Parameter patterns by language (static)
	•	SAS: %LET NAME=...;, &NAME, %sysfunc(...), %include configuration values.
Classify any &MACRO_VAR or %LET symbol as Parameter.
	•	Python: function arguments, module constants UPPER_SNAKE, dicts like CONFIG = {...}, environment reads os.getenv, pathlib.Path(...) constants.
Treat these as Parameter when used in expressions but never assigned from a dataframe column.
	•	R: function arguments, options(), Sys.getenv(), top-level constants CONST <- ....
	•	SQL: bind variables :p_term, ${TERM}, @term (server variables), CTE anchors that are literal constants.

Code-only rule for term_plus_12
	•	If we find term_plus_12 assigned as Term + 12 and Term is never assigned in code and is not a Parameter, treat Term as Input and term_plus_12 as Created.
	•	If Term is a Parameter by the patterns above, then term_plus_12 is Created sourced from a Parameter, not an Input.

UI representation: keep “Variable Type” as today, but add a small Role column:
	•	Role = DatasetColumn for Inputs, Parameter for parameters, Computed for Created/Intermediate.
This preserves your schema and improves clarity without needing data files.

⸻

3) SAS looped families like average_ead_i vs Python single vector

With code only, infer families from token patterns.

Detect families statically
	•	SAS:
	•	array avg_ead{n} avg_ead1-avg_eadn; or avg_ead{i} = ...; do i=1 to n; ...; end;
	•	Create a family key average_ead_{i} with resolved=False if n is not a literal, or construct members if literal.
	•	Python:
	•	Expansion: for i in range(n): df[f'average_ead_{i}'] = ... → same family
	•	Single vector: df['average_ead'] = ... → not a family, just one Created variable
	•	R/data.table: DT[, paste0("average_ead_", i) := ...] in loops → family

Harmonised representation
	•	Internally store:

families['average_ead_{i}'] = {
    'members': ['average_ead_1', 'average_ead_2', ...] or None,
    'language': 'sas'|'python'|'r',
    'resolved': bool
}


	•	If Python does not expand and SAS does, we still show a single collapsed node in the viz when unresolved, or a cluster when we can list members. This keeps cross-language graphs consistent without reading data.

Classification: all family members are Created. If a subset is written to a sink, only those members are “promoted,” the rest remain Intermediate. When unresolved, treat the family node as Created, Intermediate unless promoted.

⸻

4) Prevent “dataset added to variable” mix-ups, using code-only context

We must keep dataset symbols separate from variable/column names purely through parsing.

Maintain disjoint symbol tables
	•	Datasets (frames/tables):
	•	SAS: left of DATA out;, names in SET, MERGE, PROC SQL FROM, CREATE TABLE out AS
	•	Python: variables assigned DataFrame‑like objects via pd.read_*, pd.DataFrame(...), df = df.merge(...), df = other_df[...]
	•	R: read.csv/fread/arrow::read_* targets, DT <- data.table(...)
	•	SQL: table names in FROM, outputs in CREATE TABLE/VIEW, INSERT INTO
	•	Variables (columns):
	•	LHS column assignments, SELECT aliases, mutate targets, subsetting df['col']
	•	Parameters: from §2 rules

Guardrails
	•	Do not add edges from a dataset node directly to a variable unless that variable is referenced in a SELECT/KEEP list or inside a block whose active dataset context is that dataset.
	•	Keep a current dataset context stack: e.g., SAS DATA step sets out context, SET in; sets inbound context; Python block with df = ... sets dataset symbol for subsequent df['col'] references.
	•	Disallow dataset names from entering the variable namespace. If a token matches a known dataset symbol and appears in a column position, treat it as a context marker not a variable.

Outcome: dataset bubbles and variable boxes stay cleanly separated without needing to inspect any actual files.

⸻

Minimal implementation plan you can drop in next

A) Add a code-only symbol pass (no LLM):
	•	Build:
	•	assigns[var] = {'line': n, 'rhs': text, 'lang': 'sas|py|r|sql'}
	•	reads[var] = {line_numbers...}
	•	sinks = {'promoted_vars': set([...])} extracted from last writes/returns/SELECT lists
	•	datasets = {'in': set(), 'out': set(), 'symbols': set([...])}
	•	parameters = set([...]) via patterns in §2
	•	families per §3

B) Deterministic classification layer (replaces wobbly AI bucket choice):
	•	Use the 6‑step rule in §1 to produce input_variables, created_variables, intermediate_variables, all_variables, plus a roles dict mapping each name to Parameter|DatasetColumn|Computed.

C) Visualizer tweaks (tiny):
	•	Add a Role column to the lineage table.
	•	Render family nodes as collapsed clusters with a small “expand” control; when collapsed, show the template average_ead_{i}.

D) Keep LLM usage only for natural‑language fields:
	•	“Calculation Methodology”, “Explanation”, “Business Purpose” can still be LLM generated, but the buckets and edges must come from the deterministic pass to avoid flicker.

⸻

Sanity checks you can run now
	•	Run twice on the same scripts and hash the tuple:

hash((
  sorted(input_variables),
  sorted(created_variables),
  sorted(intermediate_variables),
  sorted(promoted_vars)
))

It should be identical across runs.

	•	Confirm term_plus_12 sources:
	•	If Term appears only as a token on RHS and not assigned, and not a Parameter → Input
	•	If Term is %LET TERM=... or os.getenv("TERM") → Parameter
	•	Confirm notice_of_default_fee status:
	•	If referenced in a write, export, or final select → Created (promoted)
	•	Otherwise → Intermediate

If you want, Master, tell me which file defines DataLineageAnalyzer and I will provide a compact, numbered patch set for that file only, implementing the deterministic pass and the role column, without touching your UI layout.