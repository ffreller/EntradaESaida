import pandas as pd
from datetime import datetime
from darts import TimeSeries
from darts.models import ExponentialSmoothing, Prophet
from defs import get_best_args_for_model, get_ensemble_predicitions_from_column_name, custom_add_and_subtract, create_final_dataset, print_with_time


def create_predictions():
    fim_ano = pd.read_pickle('data/interim/fim_ano.pickle')
    after_holidays = pd.read_pickle('data/interim/after_holidays.pickle')
    holidays = pd.read_pickle('data/interim/holidays.pickle')
    covariates = after_holidays.merge(fim_ano, on='ds')

    all_models = ['entrada_ui', 'entrada_uti', 'saida_ui', 'saida_uti']
    final = pd.DataFrame()
    for model in all_models:
        print_with_time(f'Gerando predições: {model}')
        #Loading data
        tipo, setor = model.split('_')
        df0 = pd.read_pickle(f'data/interim/{model}.pickle')
        series = TimeSeries.from_dataframe(df0, 'ds', 'y')
        covariates_series = TimeSeries.from_dataframe(covariates, 'ds', ['after_holidays', 'fim_ano'])
        #Exponential Smoothing
        model_es = ExponentialSmoothing(random_state=42)
        model_es.fit(series)
        preds_es = model_es.predict(7)
        y_es = preds_es.pd_series()
        
        #Loading arguments for Prophet and ensemble
        country_holidays, future_covariates, addSegunda, addTerca, subSabado, subDomingo, addAfterHolidays, subFimAno, melhor_coluna, ae90 = \
            get_best_args_for_model(model, holidays=holidays, covariates_series=covariates_series)
            
        #Prophet
        model_pr = Prophet(weekly_seasonality=True, daily_seasonality=False, yearly_seasonality=True, holidays=country_holidays)
        model_pr.fit(series, future_covariates=future_covariates)
        preds_pr = model_pr.predict(7, future_covariates=future_covariates)
        y_pr = preds_pr.pd_series()
        
        ensemble_preds = get_ensemble_predicitions_from_column_name(column_name = melhor_coluna, col_es=y_es, col_pr=y_pr)
        df1 = covariates.merge(ensemble_preds.rename('ensemble_preds'), left_on='ds', right_index=True, how='right')
        
        if 'weekday' in melhor_coluna:
            df1 = custom_add_and_subtract(df1, addSegunda, addTerca, subSabado, subDomingo, addAfterHolidays, subFimAno)
        
        this_final = create_final_dataset(df1, tipo=tipo, setor=setor, ae90=ae90)
        final = final.append(this_final)

    final['dt_carga'] = datetime.now()
    final = final[['cd_estabelecimento', 'dt_carga', 'tipo', 'ds_classific_setor', 'ds_especialidade', 'dt_previsao',
                'hrr_previsao', 'qtd_previsao','qtd_previsao_min', 'qtd_previsao_max']].reset_index(drop=True)
    final.loc[final['qtd_previsao_min'] < 0, 'qtd_previsao_min'] = 0

    final.to_pickle('data/final/todas_previsoes.pickle')
    
    todas_preds = pd.read_pickle('data/final/todas_previsoes.pickle')
    todas_preds = todas_preds.append(final)
    todas_preds.to_pickle('data/final/todas_previsoes.pickle')
    
    print_with_time('Previsões criadas com sucesso')
    

if __name__ == '__main__':
    create_predictions()
