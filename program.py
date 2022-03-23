from dbcomms import retrieve_data_from_dbprod, register_movimentacoes_realizadas_dbteste, register_predictions_dbteste
from data_preparation import preprocess_hospital_data, preprocess_external_data
from predict import create_predictions
from defs import print_with_time
from os import path as os_path, mkdir, makedirs



if __name__ == '__main__':
    if not os_path.exists('data'):
        makedirs('data/raw')
        mkdir('data/interim')
        mkdir('data/final')
    retrieve_data_from_dbprod()
    preprocess_hospital_data()
    preprocess_external_data()
    register_movimentacoes_realizadas_dbteste()
    create_predictions()
    print_with_time('Fim. Sucesso ao executar script\n')
    # register_predictions_dbteste()