import pandas as pd
import numpy as np


input_path = "\\\\uknasdata08\\FSSHARED\\WIP\\Credit Risk Assurance\\Engagements\\YYY\\YE25\\4. Data and Codes\\ECL Engine Code\\MNF\\Recoded_XXX\\Output\\6\\"
output_path = "\\\\uknasdata08\\FSSHARED\\WIP\\Credit Risk Assurance\\Engagements\\YYY\\YE25\\4. Data and Codes\\ECL Engine Code\\MNF\\Recoded_XXX\\Output\\7a\\"

df = pd.read_csv(input_path + f'MNFDataPrep_A.csv')
dftmp = pd.read_csv(input_path + f'MNFDataPrep_TEMP.csv')

# ============================================
# DEFINE LOOP PARAMETERS
# ============================================
# Calculate Max_Loops equivalent to SAS global macro: min(max(Term+12), 120)
df['term_plus_12'] = df['Term'] + 12
max_loops = int(np.minimum(df['term_plus_12'].max(), 120))

print(f"Max_Loops : {max_loops}")

# Define constants
UNPAID_DD_CHARGE = 5
FIRST_REMINDER_FEE = 15
NOTICE_OF_DEFAULT_FEE = 15

# Calculate Payments to Miss logic based on New_MIA
df['Pmts_to_Miss'] = np.where(dftmp['New_MIA'] == 0, 3,
                     np.where(dftmp['New_MIA'] == 1, 2,
                     np.where(dftmp['New_MIA'] == 2, 1, 0)))


# ============================================
# DEFINE VECTORIZED OPERATIONS
# ============================================
def calculate_arrays_vectorized(df):
    # DataFrame to store results
    results = pd.DataFrame(index=df.index)
    
    # Vectorized operations for each loop index
    for i in range(1, max_loops + 1):
        
        # ============================================
        # BASIC CALCULATIONS
        # ============================================
        if i == 1:
            TOB_Forecast = df['TOB'] + 1
            Interest = df['Current_Balance'] * df['Final_IR']
            EOM_Balance = np.maximum(df['Current_Balance'] - df['Instalment_Amount_Base'] + Interest, 0)
        else:
            TOB_Forecast = results[f'TOB_Forecast{i-1}'] + 1
            Interest = results[f'EOM_Balance{i-1}'] * df['Final_IR']
            EOM_Balance = np.maximum(results[f'EOM_Balance{i-1}'] - df['Instalment_Amount_Base'] + Interest, 0)
        
        # ============================================
        # DELINQUENCY ADJUSTMENT - BALANCE COMPONENT
        # ============================================
        if i == 1:
            EOM_Balance_X = np.where(
                df['Pmts_to_Miss'] == 0,
                df['Current_Balance'],
                np.where(
                    df['Pmts_to_Miss'] == 1,
                    df['Current_Balance'] + UNPAID_DD_CHARGE + NOTICE_OF_DEFAULT_FEE,
                    np.where(
                        df['Pmts_to_Miss'] == 2,
                        df['Current_Balance'] + UNPAID_DD_CHARGE,
                        np.where(
                            df['Pmts_to_Miss'] == 3,
                            df['Current_Balance'] + UNPAID_DD_CHARGE + FIRST_REMINDER_FEE,
                            df['Current_Balance']
                        )
                    )
                )
            )
        elif i == 2:
            EOM_Balance_X = np.where(
                df['Pmts_to_Miss'] == 0,
                EOM_Balance,
                np.where(
                    df['Pmts_to_Miss'] == 1,
                    results[f'EOM_Balance{i-1}'],
                    np.where(
                        df['Pmts_to_Miss'] == 2,
                        results[f'EOM_Balance{i-1}'] + UNPAID_DD_CHARGE + NOTICE_OF_DEFAULT_FEE,
                        np.where(
                            df['Pmts_to_Miss'] == 3,
                            results[f'EOM_Balance{i-1}'] + UNPAID_DD_CHARGE,
                            EOM_Balance
                        )
                    )
                )
            )
        elif i == 3:
            EOM_Balance_X = np.where(
                df['Pmts_to_Miss'] == 0,
                EOM_Balance,
                np.where(
                    df['Pmts_to_Miss'] == 1,
                    results[f'EOM_Balance{i-1}'],
                    np.where(
                        df['Pmts_to_Miss'] == 2,
                        results[f'EOM_Balance{i-2}'],
                        np.where(
                            df['Pmts_to_Miss'] == 3,
                            results[f'EOM_Balance{i-2}'] + UNPAID_DD_CHARGE + NOTICE_OF_DEFAULT_FEE,
                            EOM_Balance
                        )
                    )
                )
            )
        else:  # i > 3
            EOM_Balance_X = np.where(
                df['Pmts_to_Miss'] == 0,
                EOM_Balance,
                np.where(
                    df['Pmts_to_Miss'] == 1,
                    results[f'EOM_Balance{i-1}'],
                    np.where(
                        df['Pmts_to_Miss'] == 2,
                        results[f'EOM_Balance{i-2}'],
                        np.where(
                            df['Pmts_to_Miss'] == 3,
                            results[f'EOM_Balance{i-3}'],
                            EOM_Balance
                        )
                    )
                )
            )
        
        # ============================================
        # DELINQUENCY ADJUSTMENT - INTEREST COMPONENT
        # ============================================
        if i == 1:
            Final_IRR = np.where(
                df['Pmts_to_Miss'].isin([0, 1]),
                1 + df['Final_IR'],
                np.where(
                    df['Pmts_to_Miss'] == 2,
                    1 + df['Final_IR'],
                    np.where(
                        df['Pmts_to_Miss'] == 3,
                        1 + df['Final_IR'],
                        1 + df['Final_IR']
                    )
                )
            )
        elif i == 2:
            Final_IRR = np.where(
                df['Pmts_to_Miss'].isin([0, 1]),
                1 + df['Final_IR'],
                np.where(
                    df['Pmts_to_Miss'] == 2,
                    (1 + df['Final_IR']) * (1 + df['Final_IR']),
                    np.where(
                        df['Pmts_to_Miss'] == 3,
                        (1 + df['Final_IR']) * (1 + df['Final_IR']),
                        1 + df['Final_IR']
                    )
                )
            )
        else:  # i > 2
            Final_IRR = np.where(
                df['Pmts_to_Miss'].isin([0, 1]),
                1 + df['Final_IR'],
                np.where(
                    df['Pmts_to_Miss'] == 2,
                    (1 + df['Final_IR']) * (1 + df['Final_IR']),
                    np.where(
                        df['Pmts_to_Miss'] == 3,
                        (1 + df['Final_IR']) * (1 + df['Final_IR']) * (1 + df['Final_IR']),
                        1 + df['Final_IR']
                    )
                )
            )
        
        # ============================================
        # FINAL ADJUSTED EAD BALANCE
        # ============================================
        Expected_EAD = np.maximum(0, EOM_Balance_X) * Final_IRR
        
        # ============================================
        # EAD MI VARIABLES
        # ============================================
        if i == 1:
            Cumulative_EAD = Expected_EAD
            Average_EAD = Expected_EAD
        else:
            Cumulative_EAD = results[f'Cumulative_EAD{i-1}'] + Expected_EAD
            Average_EAD = Cumulative_EAD / i
        
        # ============================================
        # ADD RESULTS TO DATAFRAME (All arrays use same Max_Loops boundary)
        # ============================================
        results[f'TOB_Forecast{i}'] = TOB_Forecast
        results[f'Interest{i}'] = Interest
        results[f'EOM_Balance{i}'] = EOM_Balance
        results[f'EOM_Balance_X{i}'] = EOM_Balance_X
        results[f'Final_IRR{i}'] = Final_IRR
        results[f'Expected_EAD{i}'] = Expected_EAD
        results[f'Cumulative_EAD{i}'] = Cumulative_EAD
        results[f'Average_EAD{i}'] = Average_EAD
    
    # ============================================
    # CALCULATE FINAL EAD VALUES
    # ============================================
    # 12m EAD calculation
    results['EAD_12m'] = np.where(
        df['PD_12m'] == 1,
        df['Current_Balance'],
        np.where(
            (df['Term'] - df['TOB']) > 12,
            results['Average_EAD12'] if 'Average_EAD12' in results.columns else df['Current_Balance'],
            # For RemainingTerm <= 12, use Average_EAD(RemainingTerm)
            pd.Series([results[f'Average_EAD{int(min(max(rt, 1), max_loops))}'].iloc[idx] 
                      if f'Average_EAD{int(min(max(rt, 1), max_loops))}' in results.columns 
                      else df['Current_Balance'].iloc[idx] 
                      for idx, rt in enumerate(df['RemainingTerm'])], 
                     index=df.index)
        )
    )
    
    # Lifetime Average EAD calculation
    results['EAD_LT'] = pd.Series([results[f'Average_EAD{int(min(max(rt, 1), max_loops))}'].iloc[idx] 
                                  if f'Average_EAD{int(min(max(rt, 1), max_loops))}' in results.columns 
                                  else df['Current_Balance'].iloc[idx] 
                                  for idx, rt in enumerate(df['RemainingTerm'])],
                                 index=df.index)
    
    # Concatenate original DataFrame with results
    all_results_df = pd.concat([df, results], axis=1)
    return all_results_df

# ============================================
# APPLY THE FUNCTION TO THE DATAFRAME AND SAVE
# ============================================

final_df = calculate_arrays_vectorized(df)


# Identify columns to drop (intermediate array columns)
columns_to_drop = []

# Get all column names that match the patterns to drop
for col in final_df.columns:
    if any(pattern in col for pattern in ['TOB_Forecast', 'EOM_Balance', 'EOM_Balance_X', 
                                         'Interest', 'Final_IRR', 'Cumulative_EAD', 'Average_EAD']):

        if not col.startswith('Expected_EAD'):
            columns_to_drop.append(col)

# Drop the identified columns
final_df = final_df.drop(columns=columns_to_drop)

final_df.to_csv(output_path + 'Base_Values_EAD.csv')
