#Preprocessing of data
def get_categories( database, column ):
    labels = [x if x == x else "empty" for x in database[column].value_counts(dropna=False).index]
    return dict(zip(labels,range(len(labels))))

#Change categorical_columns for values for that category
def change_category(database,col):
    category = get_categories(database,col)
    clean_database = database.copy()
    clean_database[col] = clean_database[col].replace(category)
    return clean_database

