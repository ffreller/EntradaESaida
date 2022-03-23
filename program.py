from dbcomms import retrieve_data_from_dbprod, register_movimentacoes_realizadas_dbteste, register_predictions_dbteste
from data_preparation import preprocess_hospital_data, preprocess_external_data
from predict import create_predictions
from defs import print_with_time


if __name__ == '__main__':       
    retrieve_data_from_dbprod()
    preprocess_hospital_data()
    preprocess_external_data()
    register_movimentacoes_realizadas_dbteste()
    create_predictions()
    print_with_time('Fim. Sucesso ao executar script\n')
    # register_predictions_dbteste()