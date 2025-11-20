import pandas as pd
from etl_pipeline import transform_data 



def test_transform_data_dates_conversion():
    # Données test
    data = {'user_id': [1], 'signup_date': ['2023-01-01'], 'email': ['test@example.com'], 'age': [30]}
    df = pd.DataFrame(data)

    # Appeler la fonction à tester
    transformed_df = transform_data(df)

    # Assert: Vérifier le résultat attendu
    assert pd.api.types.is_datetime64_any_dtype(transformed_df['signup_date'])
    assert transformed_df['signup_date'][0].year == 2023

def test_transform_data_email_lowercase():
    # Données test
    data = {'user_id': [1], 'signup_date': ['2023-01-01'], 'email': ['TEST@Example.com'], 'age': [30]}
    df = pd.DataFrame(data)

    # Appeler la fonction à tester
    transformed_df = transform_data(df)

    # Assert: Vérifier le résultat attendu
    assert transformed_df['email'][0] == 'test@example.com'


def test_transform_data_age_null_or_invalid():
    # Données test
    data = {
        'user_id': [1, 2, 3],
        'signup_date': ['2023-01-01', '2023-02-01', '2023-03-01'],
        'email': ['user1@example.com', 'user2@example.com', 'user3@example.com'],
        'age': [30, None, -5] 
    }
    df = pd.DataFrame(data)

    # Appeler la fonction à tester
    transformed_df = transform_data(df)

    # Assert: Vérifier que les valeurs nulles ou invalides sont correctement gérées
    assert transformed_df.loc[1, 'age'] == 0, "L'âge nul doit être transformé en 0"
    assert transformed_df.loc[2, 'age']  == 0, "L'âge négatif doit être transformé en 0"

    # Vérifier que les valeurs valides restent inchangées
    assert transformed_df.loc[0, 'age'] == 30