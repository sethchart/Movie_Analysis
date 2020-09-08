from moviesdb import moviesdb as mdb

tbap = mdb._TitleBasicsParser()
tbap.import_data_titles()
trp = mdb._TitleRatingsParser()
trp.import_data_ratings()
tbup = mdb._TitleBudgetsParser()
tbup.import_data_budgets()
nbp = mdb._NameBasicsParser()
nbp.import_data_names()
pp = mdb._PrincipalsParser()
pp.import_data_principals()
