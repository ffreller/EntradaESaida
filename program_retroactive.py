from dbcomms import retrieve_data_from_dbprod, register_movimentacoes_realizadas_dbteste, register_predictions_dbteste
from data_preparation import preprocess_hospital_data_with_filter, preprocess_external_data
from predict import create_predictions
from defs import print_with_time
from datetime import date
from pandas import date_range, to_datetime

    
def ExecuteProgram(start_date, end_date):
    print()
    retrieve_data_from_dbprod()
    days_to_simulate = date_range(start_date, end_date, freq='d')
    for day in days_to_simulate:
        print_with_time(f'Simulando dia: {day.strftime("%d/%m/%Y")}')
        preprocess_hospital_data_with_filter(day)
        preprocess_external_data()
        register_movimentacoes_realizadas_dbteste()
        create_predictions()
        register_predictions_dbteste()
        print_with_time('Fim. Sucesso ao executar script')
        print()

    
if __name__ == '__main__':
    dia_str = '01/03/2022'
    this_start_date = to_datetime(dia_str, format="%d/%m/%Y")
    this_end_date = date.today()
    print('*'*100)
    print(f'Simulando de {dia_str} até {this_end_date.strftime("%d/%m/%Y")}')
    print('*'*100)
    ExecuteProgram(this_start_date, this_end_date)
    