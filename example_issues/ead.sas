%let input_path = "\\uknasdata08\FSSHARED\WIP\Credit Risk Assurance\Engagements\YYY\YE25\4. Data and Codes\ECL Engine Code\MNF\Recoded_XXX\Output\6\";
    %let output_path = "\\uknasdata08\FSSHARED\WIP\Credit Risk Assurance\Engagements\YYY\YE25\4. Data and Codes\ECL Engine Code\MNF\Recoded_XXX\Output\7a\";

    /* Read CSV files */
    proc import datafile="&input_path.MNFDataPrep_A.csv" 
        out=df 
        dbms=csv 
        replace;
        getnames=yes;
    run;

    proc import datafile="&input_path.MNFDataPrep_TEMP.csv" 
        out=dftmp 
        dbms=csv 
        replace;
        getnames=yes;
    run;

    /* Calculate Max Loops */
    data df;
        set df;
        term_plus_12 = Term + 12;
    run;

    proc sql noprint;
        select min(max(term_plus_12), 120) into :max_loops from df;
    quit;

    %put Max_Loops = &max_loops;

    /* Define constants */
    %let UNPAID_DD_CHARGE = 5;
    %let FIRST_REMINDER_FEE = 15;
    %let NOTICE_OF_DEFAULT_FEE = 15;

    /* Calculate Pmts_to_Miss */
    data df;
        merge df(in=a) dftmp(in=b);
        by _all_;
        if b then do;
            if New_MIA = 0 then Pmts_to_Miss = 3;
            else if New_MIA = 1 then Pmts_to_Miss = 2;
            else if New_MIA = 2 then Pmts_to_Miss = 1;
            else Pmts_to_Miss = 0;
        end;
    run;

    /* Define vectorized operations */
    %macro calculate_arrays_vectorized(max_loops);
        %do i = 1 %to &max_loops;
            data results;
                set df;

                /* Basic calculations */
                if &i = 1 then do;
                    TOB_Forecast = TOB + 1;
                    Interest = Current_Balance * Final_IR;
                    EOM_Balance = max(Current_Balance - Instalment_Amount_Base + Interest, 0);
                end;
                else do;
                    TOB_Forecast = TOB_Forecast&i-1 + 1;
                    Interest = EOM_Balance&i-1 * Final_IR;
                    EOM_Balance = max(EOM_Balance&i-1 - Instalment_Amount_Base + Interest, 0);
                end;

                /* Delinquency adjustment - balance component */
                if &i = 1 then do;
                    EOM_Balance_X = ifn(Pmts_to_Miss = 0, Current_Balance,
                                      ifn(Pmts_to_Miss = 1, Current_Balance + &UNPAID_DD_CHARGE + &NOTICE_OF_DEFAULT_FEE,
                                      ifn(Pmts_to_Miss = 2, Current_Balance + &UNPAID_DD_CHARGE,
                                      ifn(Pmts_to_Miss = 3, Current_Balance + &UNPAID_DD_CHARGE + &FIRST_REMINDER_FEE,
                                      Current_Balance))));
                end;
                else if &i = 2 then do;
                    EOM_Balance_X = ifn(Pmts_to_Miss = 0, EOM_Balance,
                                      ifn(Pmts_to_Miss = 1, EOM_Balance&i-1,
                                      ifn(Pmts_to_Miss = 2, EOM_Balance&i-1 + &UNPAID_DD_CHARGE + &NOTICE_OF_DEFAULT_FEE,
                                      ifn(Pmts_to_Miss = 3, EOM_Balance&i-1 + &UNPAID_DD_CHARGE,
                                      EOM_Balance))));
                end;
                else if &i = 3 then do;
                    EOM_Balance_X = ifn(Pmts_to_Miss = 0, EOM_Balance,
                                      ifn(Pmts_to_Miss = 1, EOM_Balance&i-1,
                                      ifn(Pmts_to_Miss = 2, EOM_Balance&i-2,
                                      ifn(Pmts_to_Miss = 3, EOM_Balance&i-2 + &UNPAID_DD_CHARGE + &NOTICE_OF_DEFAULT_FEE,
                                      EOM_Balance))));
                end;
                else do;
                    EOM_Balance_X = ifn(Pmts_to_Miss = 0, EOM_Balance,
                                      ifn(Pmts_to_Miss = 1, EOM_Balance&i-1,
                                      ifn(Pmts_to_Miss = 2, EOM_Balance&i-2,
                                      ifn(Pmts_to_Miss = 3, EOM_Balance&i-3,
                                      EOM_Balance))));
                end;

                /* Delinquency adjustment - interest component */
                if &i = 1 then do;
                    Final_IRR = ifn(Pmts_to_Miss in (0, 1), 1 + Final_IR,
                                  ifn(Pmts_to_Miss = 2, 1 + Final_IR,
                                  ifn(Pmts_to_Miss = 3, 1 + Final_IR,
                                  1 + Final_IR)));
                end;
                else if &i = 2 then do;
                    Final_IRR = ifn(Pmts_to_Miss in (0, 1), 1 + Final_IR,
                                  ifn(Pmts_to_Miss = 2, (1 + Final_IR) * (1 + Final_IR),
                                  ifn(Pmts_to_Miss = 3, (1 + Final_IR) * (1 + Final_IR),
                                  1 + Final_IR)));
                end;
                else do;
                    Final_IRR = ifn(Pmts_to_Miss in (0, 1), 1 + Final_IR,
                                  ifn(Pmts_to_Miss = 2, (1 + Final_IR) * (1 + Final_IR),
                                  ifn(Pmts_to_Miss = 3, (1 + Final_IR) * (1 + Final_IR) * (1 + Final_IR),
                                  1 + Final_IR)));
                end;

                /* Final adjusted EAD balance */
                Expected_EAD = max(0, EOM_Balance_X) * Final_IRR;

                /* EAD MI variables */
                if &i = 1 then do;
                    Cumulative_EAD = Expected_EAD;
                    Average_EAD = Expected_EAD;
                end;
                else do;
                    Cumulative_EAD = Cumulative_EAD&i-1 + Expected_EAD;
                    Average_EAD = Cumulative_EAD / &i;
                end;

                /* Add results to the dataset */
                TOB_Forecast&i = TOB_Forecast;
                EOM_Balance&i = EOM_Balance;
                EOM_Balance_X&i = EOM_Balance_X;
                Final_IRR&i = Final_IRR;
                Expected_EAD&i = Expected_EAD;
                Cumulative_EAD&i = Cumulative_EAD;
                Average_EAD&i = Average_EAD;
            run;
        %end;

        /* Calculate final EAD values */
        data final_df;
            set results;
            if PD_12m = 1 then EAD_12m = Current_Balance;
            else if (Term - TOB) > 12 then EAD_12m = Average_EAD12;
            else do;
                RemainingTerm = min(max(RemainingTerm, 1), &max_loops);
                EAD_12m = Average_EAD;
            end;

            RemainingTerm = min(max(RemainingTerm, 1), &max_loops);
            EAD_LT = Average_EAD;
        run;

        /* Drop intermediate columns */
        proc datasets library=work;
            modify final_df;
            drop TOB_Forecast1-TOB_Forecast&max_loops EOM_Balance1-EOM_Balance&max_loops 
                 EOM_Balance_X1-EOM_Balance_X&max_loops Final_IRR1-Final_IRR&max_loops 
                 Cumulative_EAD1-Cumulative_EAD&max_loops Average_EAD1-Average_EAD&max_loops;
        quit;

    /* Save the final dataset */
    proc export data=final_df 
        outfile="&output_path.Base_Values_EAD.csv" 
        dbms=csv 
        replace;
        putnames=yes;
    run;
  

