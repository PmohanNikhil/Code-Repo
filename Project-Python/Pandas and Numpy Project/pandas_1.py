import pandas as pd


csv_path = "file1.csv"

df = pd.read_csv(csv_path)

print(df)

df.head() #Prints the first 5 rows
df.tail() #Prints the last 5 rows


dict_data = {
    "Artist": ["Picasso", "Van Gogh", "Da Vinci"],
    "Year": [1905, 1889, 1503],
    "Style": ["Cubism", "Post-Impressionism", "Renaissance"],
    "Price": [2000000, 3000000, 4000000],
    "Location": ["Paris", "Amsterdam", "Florence"],
    "Medium": ["Oil", "Oil", "Oil"],
    "Sold": [True, False, True],
    "SoundTrack": [None, None, None]
}

df = pd.DataFrame(dict_data)

print(df)

y = df[["Artist", "Year", "Style"]]

print(y)

print(df.iloc[0,0]) # Accessing a particular element (row 0, column 0)
print(df.loc[0, "Artist"]) # Accessing a particular element (row 0, column "Artist")
print(df[df["Price"] > 2500000]) # Filtering rows based on a condition
print(df[df["Artist"].isin(["Picasso", "Da Vinci"])]) # Filtering rows based on multiple values

#To find the element in the second row and first column
print(df.iloc[1,0
              ])