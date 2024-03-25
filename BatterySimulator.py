import gspread
import pandas as pd
import os
import numpy as np
from multiprocessing import Pool
from HoymilesMain import getHoymilesData
from gspread_dataframe import set_with_dataframe

class setup:
    CAPACITY_FACTOR = 0.9
    def __init__(self, MaxCapacity, Inverter,Factor = 1):
        self.MaxCapacity = MaxCapacity * self.CAPACITY_FACTOR
        self.Inverter = Inverter
        self.Factor = Factor
    def __repr__(self):
        return '{:.0f} kWh\n{:.0f} W\nFac: {:.1f}'.format(self.MaxCapacity/self.CAPACITY_FACTOR, self.Inverter, self.Factor)

# creating list
Setups = []
Setups.append(setup(1000,400,1.9))
Setups.append(setup(1500,400,1.9))
Setups.append(setup(2000,400,1.9))
Setups.append(setup(2400,400,1.9))
Setups.append(setup(float('inf'),400,1.9))

def init_worker(igc, iTotals, iBlindEnergyInPhase, iTotalEnergyProduced, idf, iResData):
    global gc, Totals, BlindEnergyInPhase, TotalEnergyProduced, df, ResData
    gc = igc
    Totals = iTotals
    BlindEnergyInPhase = iBlindEnergyInPhase
    TotalEnergyProduced = iTotalEnergyProduced
    df = idf
    ResData = iResData

def calcBattery(Setup):
    print('Calculating {:s}'.format(repr(Setup).replace('\n',' ')))
    BatteryDf = df[['Timestamp']].copy()
    BatteryDf['EnergyStored'] = 0.0
    BatteryDf['EnergyUsed'] = 0.0
    # iterate over batteries
    MaxCapacity = Setup.MaxCapacity  
    Inverter = Setup.Inverter
    Factor = Setup.Factor

    EnergyReturned = 0
    BatteryEnergyUsed = 0
    MaxCapacityReached = 0
    NumMaxCapacityReached = 0
    NumMaxCapacityReachedSet = False
    NumCycles = 0
    NumCyclesSet = False

    for ind in df.index:
        Energy = df['TotalConsumed'][ind] - Factor * df['TotalReturned'][ind]
        BatteryDf.loc[ind,"EnergyUsed"] = min(df['TotalConsumed'][ind],Factor * df['TotalReturned'][ind])
        if ind == 0:
            BatteryDf.loc[ind,"EnergyStored"] = 0
        elif (Energy < 0):
            BatteryDf.loc[ind,"EnergyStored"] = BatteryDf['EnergyStored'][ind-1] - Energy
            if (BatteryDf['EnergyStored'][ind] > MaxCapacity):
                EnergyReturned = EnergyReturned + BatteryDf['EnergyStored'][ind] - MaxCapacity
                BatteryDf.loc[ind,"EnergyStored"] = MaxCapacity
                if not NumMaxCapacityReachedSet:
                    NumMaxCapacityReached = NumMaxCapacityReached+1
                    NumMaxCapacityReachedSet = True
        elif (BatteryDf['EnergyStored'][ind-1] > 0):
            MaxPosibleEnergy = min(Inverter/60,Energy)
            BatteryDf.loc[ind,"EnergyStored"] = BatteryDf['EnergyStored'][ind-1] - MaxPosibleEnergy
            if (BatteryDf['EnergyStored'][ind] > 0):
                BatteryEnergyUsed = BatteryEnergyUsed + MaxPosibleEnergy
            else:
                BatteryEnergyUsed = BatteryEnergyUsed + BatteryDf['EnergyStored'][ind] + MaxPosibleEnergy
                BatteryDf.loc[ind,"EnergyStored"] = 0
                if not NumCyclesSet:
                    NumCycles = NumCycles+1
                    NumCyclesSet = True
            
        if (BatteryDf['EnergyStored'][ind]>(MaxCapacity/4)):
            NumCyclesSet = False
        
        if (BatteryDf['EnergyStored'][ind]<(MaxCapacity/2)):
            NumMaxCapacityReachedSet = False
    
    MaxCapacityReached = BatteryDf['EnergyStored'].max()
    dfDay = BatteryDf.set_index(pd.DatetimeIndex(BatteryDf["Timestamp"])).drop(['Timestamp'],axis=1).resample('D').max()
    # BatteryDf = BatteryDf.rename(columns={"EnergyStored": repr(Setup)})
    TotalEnergyUsedNoBattery = BatteryDf['EnergyUsed'].sum()
    TotalConsumed = df['TotalConsumed'].sum()

    Results = list()
    Results.append('%.0f kWh' % TotalEnergyUsedNoBattery)
    Results.append(' %.0f kWh' % EnergyReturned)
    Results.append('%.0f kWh' % BatteryEnergyUsed)
    Results.append('%.0f kWh' % MaxCapacityReached)
    Results.append('%.0f kWh' % dfDay.EnergyStored.median())
    Results.append('%i' % NumMaxCapacityReached)
    Results.append('%i' % NumCycles)
    Results.append('%.1f %%' % ((TotalEnergyUsedNoBattery/(df['TotalReturned'].sum()*Factor) + BlindEnergyInPhase/TotalEnergyProduced)*100))
    Results.append('%.1f %%' % ((TotalEnergyUsedNoBattery/TotalConsumed + BlindEnergyInPhase/Totals['TotalConsumed'].sum())*100))
    Results.append('%.1f %%' % (BatteryEnergyUsed/TotalConsumed*100))
    Results.append('%.1f %%' % ((TotalEnergyUsedNoBattery/TotalConsumed + BlindEnergyInPhase/Totals['TotalConsumed'].sum() + BatteryEnergyUsed/TotalConsumed)*100))
    return pd.Series(data=Results, name=repr(Setup), index=ResData.index), BatteryDf['EnergyStored']

if __name__ == '__main__':
    gc = gspread.oauth(scopes=gspread.auth.DEFAULT_SCOPES,
                credentials_filename=os.path.join(os.getcwd(),"credentials.json"),
                authorized_user_filename=os.path.join(os.getcwd(),"authorized_user.json"))
    # gc = gspread.oauth_from_dict(scopes=gspread.auth.READONLY_SCOPES,credentials=)
    sh = gc.open("Shelly Data")

    TotalEnergyProduced = np.sum(getHoymilesData(['total_eq','today_eq'])) * 1000
    OvweviewSheet = sh.worksheet("Overview")
    Totals = pd.DataFrame(OvweviewSheet.get_all_records(numericise_ignore=['all']))
    Totals['TotalConsumed'] = Totals['TotalConsumed'].replace(',', '.', regex=True).astype(float)
    Totals['TotalReturned'] = Totals['TotalReturned'].replace(',', '.', regex=True).astype(float)
    BlindEnergyInPhase = TotalEnergyProduced -Totals.loc[1,'TotalReturned'] #Use Phase b for me

    ResData = pd.DataFrame(index=['Total energy used without Battery:',
        'Total energy returned:',
        'Total battery energy used:',
        'Maximal battery capacity reached:',
        'Median maximal battery capacity reached:',
        'Number of times the maximal capacity was reached:',
        'Number of battery cycles:',
        'Plant energy ratio without battery:',
        'Plant self-sufficiency without battery:',
        'Battery self-sufficiency:',
        'Overall self-sufficiency:'])

    worksheet = sh.worksheet("Total")
    df = pd.DataFrame(worksheet.get_all_records(numericise_ignore=['all'])).drop('Balance',axis=1)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    df['TotalConsumed'] = df['TotalConsumed'].replace(',', '.', regex=True).astype(float)
    df['TotalReturned'] = df['TotalReturned'].replace(',', '.', regex=True).astype(float)
    
    # create the process pool
    with Pool(initializer=init_worker, initargs=(gc, Totals, BlindEnergyInPhase, TotalEnergyProduced, df, ResData,)) as pool:
        for result in pool.imap(calcBattery, Setups):
            ResData[result[0].name] = result[0]
            df = pd.concat([df, result[1].rename(result[0].name)], axis=1)

    print(ResData)

    OutSh = gc.open("Battery Sim")
    OutSh.worksheet("Overview").clear()
    OutSh.worksheet("Data").clear()
    set_with_dataframe(OutSh.worksheet("Overview"), ResData, include_index=True)
    set_with_dataframe(OutSh.worksheet("Data"), df.drop(['TotalConsumed', 'TotalReturned'],axis=1))