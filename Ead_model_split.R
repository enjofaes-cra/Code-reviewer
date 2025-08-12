import pandas as pd
import numpy as np
from tqdm import tqdm

# =====================================================
# [0] CONFIG: paths, file names, and key
# =====================================================
input_path  = r"\\uknasdata08\FSSHARED\WIP\Credit Risk Assurance\Engagements\XXXXX\YE25\4. Data and Codes\ECL Engine Code\MNF\Recoded_Hitesh\Output\6\"
output_path = r"\\uknasdata08\FSSHARED\WIP\Credit Risk Assurance\Engagements\XXXXX\YE25\4. Data and Codes\ECL Engine Code\MNF\Recoded_Hitesh\Output\7a\"

# Three input files (edit if needed)
file_core  = "MNFDataPrep_A_core.csv"        # identifiers, structural fields, terms
file_rate  = "MNFDataPrep_B_rates.csv"       # interest rate and instalment amounts
file_deliq = "MNFDataPrep_C_delinquency.csv" # delinquency, PD_12m, MIA, remaining term

# Join key (must be present in all three inputs)
KEY_COL = "account_id"  # change to your actual join key if different, e.g., "Contract_ID"

# Map variables to each input set. Adjust if your columns live elsewhere.
VARS_CORE = [
    KEY_COL, "TOB", "Term", "Current_Balance"
]
VARS_RATE = [
    KEY_COL, "Final_IR", "Instalment_Amount_Base"
]
VARS_DELIQ = [
    KEY_COL, "New_MIA", "PD_12m", "RemainingTerm"
]

# =====================================================
# [1] LOAD & MERGE THE THREE INPUT DATASETS
# =====================================================
core  = pd.read_csv(input_path + file_core,  dtype="float64", low_memory=False)
rates = pd.read_csv(input_path + file_rate,  dtype="float64", low_memory=False)
mias  = pd.read_csv(input_path + file_deliq, dtype="float64", low_memory=False)

# Ensure key exists and keep only columns we need from each file
for nm, df_in, need in [("core", core, VARS_CORE), ("rates", rates, VARS_RATE), ("delinquency", mias, VARS_DELIQ)]:
    if KEY_COL not in df_in.columns:
        raise KeyError(f"{nm} input is missing join key '{KEY_COL}'.")
    miss = [c for c in need if c not in df_in.columns]
    if miss:
        raise KeyError(f"{nm} input is missing required columns: {miss}")

core  = core[VARS_CORE].copy()
rates = rates[VARS_RATE].copy()
mias  = mias[VARS_DELIQ].copy()

# Merge (core ⟵ rates ⟵ delinquency)
df = core.merge(rates, on=KEY_COL, how="left").merge(mias, on=KEY_COL, how="left")

# Final sanity check
REQUIRED = list(set(VARS_CORE + VARS_RATE + VARS_DELIQ))
missing_final = [c for c in REQUIRED if c not in df.columns]
if missing_final:
    raise KeyError(f"Merged dataset missing: {missing_final}")

# =====================================================
# [2] DEFINE LOOP PARAMETERS & CONSTANTS
# =====================================================
# [2.1] Calculate Max_Loops equivalent of min(max(Term+12), 120)
df["term_plus_12"] = df["Term"] + 12
max_loops = int(np.minimum(df["term_plus_12"].max(), 120))
print(f"Max_Loops : {max_loops}")

# [2.2] Constants (fees)
UNPAID_DD_CHARGE = 5.0
FIRST_REMINDER_FEE = 15.0
NOTICE_OF_DEFAULT_FEE = 15.0

# [2.3] Payments-to-Miss logic from New_MIA
#        New_MIA=0 -> 3, 1 -> 2, 2 -> 1, else -> 0
df["Pmts_to_Miss"] = np.where(df["New_MIA"] == 0, 3,
                       np.where(df["New_MIA"] == 1, 2,
                       np.where(df["New_MIA"] == 2, 1, 0)))

# =====================================================
# [3] CORE ENGINE (vectorised across rows, loop across months)
# =====================================================
def calculate_arrays_vectorized(df_in: pd.DataFrame) -> pd.DataFrame:
    """
    Run the monthly EAD projection arrays on a pre-merged DataFrame,
    writing arrays into a results frame. The loop progresses month-by-month.
    """
    # [3.1] Prepare result container aligned to input rows
    results = pd.DataFrame(index=df_in.index, dtype="float64")

    # [3.2] Month loop with a progress bar
    for i in tqdm(range(1, max_loops + 1), desc="Projecting months"):
        # --------------------------------------------
        # [3.2.1] BASIC CALCULATIONS
        # --------------------------------------------
        if i == 1:
            TOB_Forecast = df_in["TOB"] + 1
            Interest = df_in["Current_Balance"] * df_in["Final_IR"]
            EOM_Balance = np.maximum(df_in["Current_Balance"] - df_in["Instalment_Amount_Base"] + Interest, 0.0)
        else:
            TOB_Forecast = results[f"TOB_Forecast{i-1}"] + 1
            Interest = results[f"EOM_Balance{i-1}"] * df_in["Final_IR"]
            EOM_Balance = np.maximum(results[f"EOM_Balance{i-1}"] - df_in["Instalment_Amount_Base"] + Interest, 0.0)

        # --------------------------------------------
        # [3.2.2] DELINQUENCY ADJUSTMENT - BALANCE COMPONENT
        # --------------------------------------------
        if i == 1:
            EOM_Balance_X = np.where(
                df_in["Pmts_to_Miss"] == 0, df_in["Current_Balance"],
                np.where(
                    df_in["Pmts_to_Miss"] == 1, df_in["Current_Balance"] + UNPAID_DD_CHARGE + NOTICE_OF_DEFAULT_FEE,
                    np.where(
                        df_in["Pmts_to_Miss"] == 2, df_in["Current_Balance"] + UNPAID_DD_CHARGE,
                        np.where(
                            df_in["Pmts_to_Miss"] == 3, df_in["Current_Balance"] + UNPAID_DD_CHARGE + FIRST_REMINDER_FEE,
                            df_in["Current_Balance"]
                        )
                    )
                )
            )
        elif i == 2:
            EOM_Balance_X = np.where(
                df_in["Pmts_to_Miss"] == 0, EOM_Balance,
                np.where(
                    df_in["Pmts_to_Miss"] == 1, results[f"EOM_Balance{i-1}"],
                    np.where(
                        df_in["Pmts_to_Miss"] == 2, results[f"EOM_Balance{i-1}"] + UNPAID_DD_CHARGE + NOTICE_OF_DEFAULT_FEE,
                        np.where(
                            df_in["Pmts_to_Miss"] == 3, results[f"EOM_Balance{i-1}"] + UNPAID_DD_CHARGE,
                            EOM_Balance
                        )
                    )
                )
            )
        elif i == 3:
            EOM_Balance_X = np.where(
                df_in["Pmts_to_Miss"] == 0, EOM_Balance,
                np.where(
                    df_in["Pmts_to_Miss"] == 1, results[f"EOM_Balance{i-1}"],
                    np.where(
                        df_in["Pmts_to_Miss"] == 2, results[f"EOM_Balance{i-2}"],
                        np.where(
                            df_in["Pmts_to_Miss"] == 3, results[f"EOM_Balance{i-2}"] + UNPAID_DD_CHARGE + NOTICE_OF_DEFAULT_FEE,
                            EOM_Balance
                        )
                    )
                )
            )
        else:  # i > 3
            EOM_Balance_X = np.where(
                df_in["Pmts_to_Miss"] == 0, EOM_Balance,
                np.where(
                    df_in["Pmts_to_Miss"] == 1, results[f"EOM_Balance{i-1}"],
                    np.where(
                        df_in["Pmts_to_Miss"] == 2, results[f"EOM_Balance{i-2}"],
                        np.where(
                            df_in["Pmts_to_Miss"] == 3, results[f"EOM_Balance{i-3}"],
                            EOM_Balance
                        )
                    )
                )
            )

        # --------------------------------------------
        # [3.2.3] DELINQUENCY ADJUSTMENT - INTEREST COMPONENT
        # --------------------------------------------
        if i == 1:
            Final_IRR = 1 + df_in["Final_IR"]
        elif i == 2:
            Final_IRR = np.where(
                df_in["Pmts_to_Miss"].isin([0, 1]), 1 + df_in["Final_IR"],
                (1 + df_in["Final_IR"]) * (1 + df_in["Final_IR"])
            )
        else:  # i > 2
            Final_IRR = np.where(
                df_in["Pmts_to_Miss"].isin([0, 1]),
                1 + df_in["Final_IR"],
                np.where(
                    df_in["Pmts_to_Miss"] == 2,
                    (1 + df_in["Final_IR"]) * (1 + df_in["Final_IR"]),
                    (1 + df_in["Final_IR"]) * (1 + df_in["Final_IR"]) * (1 + df_in["Final_IR"])
                )
            )

        # --------------------------------------------
        # [3.2.4] FINAL ADJUSTED EAD BALANCE
        # --------------------------------------------
        Expected_EAD = np.maximum(0.0, EOM_Balance_X) * Final_IRR

        # --------------------------------------------
        # [3.2.5] EAD MOVING INDICATORS
        # --------------------------------------------
        if i == 1:
            Cumulative_EAD = Expected_EAD
            Average_EAD = Expected_EAD
        else:
            Cumulative_EAD = results[f"Cumulative_EAD{i-1}"] + Expected_EAD
            Average_EAD = Cumulative_EAD / i

        # --------------------------------------------
        # [3.2.6] STORE THIS MONTH'S ARRAYS
        # --------------------------------------------
        results[f"TOB_Forecast{i}"]   = TOB_Forecast
        results[f"Interest{i}"]       = Interest
        results[f"EOM_Balance{i}"]    = EOM_Balance
        results[f"EOM_Balance_X{i}"]  = EOM_Balance_X
        results[f"Final_IRR{i}"]      = Final_IRR
        results[f"Expected_EAD{i}"]   = Expected_EAD
        results[f"Cumulative_EAD{i}"] = Cumulative_EAD
        results[f"Average_EAD{i}"]    = Average_EAD

    # [3.3] FINAL EADS
    # 12m EAD (PD_12m==1 => take current balance)
    if "Average_EAD12" in results.columns:
        avg12 = results["Average_EAD12"].values
    else:
        # Fallback if fewer than 12 loops
        fallback_idx = np.minimum(12, max_loops)
        avg12 = results[f"Average_EAD{fallback_idx}"].values

    # For RemainingTerm <= 12 pick Average_EAD(RemainingTerm), else Average_EAD12
    def pick_avg_ead(rt, row_idx):
        k = int(min(max(rt, 1), max_loops))
        col = f"Average_EAD{k}"
        return results[col].iat[row_idx] if col in results.columns else df_in["Current_Balance"].iat[row_idx]

    ead_12m = np.where(
        df_in["PD_12m"] == 1,
        df_in["Current_Balance"].values,
        np.where(
            (df_in["Term"] - df_in["TOB"]) > 12,
            avg12,
            np.fromiter((pick_avg_ead(rt, idx) for idx, rt in enumerate(df_in["RemainingTerm"].values)),
                        dtype=float, count=len(df_in))
        )
    )

    # Lifetime EAD = Average_EAD(RemainingTerm)
    ead_lt = np.fromiter(
        (pick_avg_ead(rt, idx) for idx, rt in enumerate(df_in["RemainingTerm"].values)),
        dtype=float, count=len(df_in)
    )

    results["EAD_12m"] = ead_12m
    results["EAD_LT"]  = ead_lt

    # [3.4] RETURN CONCATENATED OUTPUT
    return pd.concat([df_in.reset_index(drop=True), results.reset_index(drop=True)], axis=1)

# =====================================================
# [4] RUN & SAVE
# =====================================================
final_df = calculate_arrays_vectorized(df)

# Drop intermediate arrays except Expected_EAD*
to_drop = []
for c in final_df.columns:
    if any(pat in c for pat in ["TOB_Forecast", "EOM_Balance", "EOM_Balance_X",
                                "Interest", "Final_IRR", "Cumulative_EAD", "Average_EAD"]):
        if not c.startswith("Expected_EAD"):
            to_drop.append(c)

final_df = final_df.drop(columns=to_drop)
final_df.to_csv(output_path + "Base_Values_EAD.csv", index=False)
print("Saved:", output_path + "Base_Values_EAD.csv")