import pandas as pd

data = {'Released':[1982,1980,1973,1992,1999,1977,1976,1976,1977,1977]}

df = pd.DataFrame(data)

print(df['Released'].unique()) #Prints unique values in the 'Released' column


df['Released'] >=1980 #Returns a boolean series where the condition is met

df1 = df[df['Released'] >=1980] #Filters the dataframe based on the condition

df1.to_csv('filtered_data.csv', index=False) #Saves the filtered dataframe to a new CSV file without the index