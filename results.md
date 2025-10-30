Por favor mejora este texto
```
El primer paso como se mostro en la metodologia, fue el analisis de los
datos y la busqueda de modelo, para eso aplicamos una metodologia de Grid search
donde para cada uno de los metodos que tenemos, definimos distintos hiperparametros
y corremos los experimentos. Estos experimentos se corriendo en una computadora con
AMD ryzen 7 y con una tarjeta grafica (integrada), el total de entrenamiento fue de
15 minutos
```

Por favor has una tabla con los siguiente
```
 "LinearRegression": (
            Pipeline([("scaler", StandardScaler()), ("model", LinearRegression())]),
            {"model__fit_intercept": [True, False]}
        ),
        "Ridge": (
            Pipeline([("scaler", StandardScaler()), ("model", Ridge(random_state=42))]),
            {"model__alpha": uniform(1e-4, 100.0)}
        ),
        "Lasso": (
            Pipeline([("scaler", StandardScaler()), ("model", Lasso(random_state=42, max_iter=10000))]),
            {"model__alpha": uniform(1e-4, 10.0)}
        ),
        "SVR": (
            Pipeline([("scaler", StandardScaler()), ("model", SVR())]),
            {
                "model__kernel": ["rbf", "linear"],
                "model__C": uniform(0.1, 100),
                "model__gamma": ["scale", "auto"]
            }
        ),
        "RandomForest": (
            RandomForestRegressor(random_state=42, n_jobs=-1),
            {
                "n_estimators": randint(100, 600),
                "max_depth": [None] + list(range(3, 15)),
                "min_samples_split": randint(2, 10),
                "min_samples_leaf": randint(1, 10),
                "max_features": ["auto", "sqrt", 0.5, 0.7, 1.0]
            }
        ),
        "XGBRegressor": (
            XGBRegressor(
                random_state=42, n_jobs=-1, tree_method="hist",
                objective="reg:squarederror"
            ),
            {
                "n_estimators": randint(200, 800),
                "max_depth": randint(3, 10),
                "learning_rate": uniform(0.01, 0.29),  # ~0.01-0.3
                "subsample": uniform(0.6, 0.4),        # 0.6-1.0
                "colsample_bytree": uniform(0.6, 0.4), # 0.6-1.0
                "min_child_weight": randint(1, 10)
            }
        ),
        "MLPRegressor": (
            Pipeline([("scaler", StandardScaler()),
                      ("model", MLPRegressor(random_state=42, max_iter=1000))]),
            {
                "model__hidden_layer_sizes": [(64,), (128,), (128, 64), (256, 128)],
                "model__activation": ["relu", "tanh"],
                "model__alpha": uniform(1e-6, 1e-2),
                "model__learning_rate_init": uniform(1e-4, 1e-2)
            }
```

Por favor resumeme esto y ponle en alguna manera mas formal para presentar resultados
```
===== Scenario: FULL FEATURES =====
Searching LinearRegression ...
Searching Ridge ...
Searching Lasso ...
Searching RandomForest ...
Searching XGBRegressor ...
Searching MLPRegressor ...
              Model   R2_test  R2_train       MAE      RMSE
4      XGBRegressor  0.983173  0.999748  0.006627  0.000865
3      RandomForest  0.982756  0.995238  0.003623  0.000887
5      MLPRegressor  0.594899  0.921455  0.102132  0.020828
0  LinearRegression  0.529671  0.483496  0.124817  0.024182
1             Ridge  0.525373  0.481882  0.126453  0.024403
2             Lasso -0.001738  0.000000  0.207379  0.051504

Best model:
Model                                              XGBRegressor
```
Explica por favor tambin los resultados de cada uno de esos, tipo que significa
cada uno de Estos

Por favor mejor esto
```
Ahora que ya tenemos el modelo que vamos a utilizar, entonces vamos a usar pickle
para conectar nuestro modelo a una base de datos, y con javascript puedo hacer llamadas
a adecuadas a la pagina web.
```



