def decyzja_kredytowa(Outstanding_Debt:float, Credit_Mix:str, Num_Credit_Card:int, Interest_Rate:int, op_good=0.05, op_standard=0.075):
    
    if Outstanding_Debt <= 1494.26:
        if Credit_Mix != 'Standard':
            if Credit_Mix == 'Bad':
                decyzja = ["Poor", 'Nie przyznano pożyczki. Powód: zbyt duża dywersyfikacja posiadanych kredytów.']
            else:
                if Num_Credit_Card <= 5.50:
                    decyzja = ["Good", op_good]
                else:
                    decyzja = ["Standard", op_standard]
        else:
            if Interest_Rate <= 20.50:
                if Num_Credit_Card <= 7.50:
                    decyzja = ["Standard", op_standard]
                else:
                    decyzja = ["Poor", 'Nie przyznano pożyczki. Powód: zbyt duża liczba posiadanych kart kredytowych przy aktualnej dywersyfikacji kredytów.']
            else:
                decyzja = ["Poor", 'Nie przyznano pożyczki. Powód: zbyt duże oprocentowanie posiadanych zobowiązań przy aktualnej dywersyfikacja kredytów.']
    else:
        if Interest_Rate <= 14.50:
            decyzja = ["Standard", op_standard]
        else:
            decyzja = ["Poor", 'Nie przyznano pożyczki. Powód: zbyt duże oprocentowanie posiadanych zobowiązań przy aktulnym długu.']
    
    return decyzja