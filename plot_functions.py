from seaborn import lineplot, scatterplot
import matplotlib.pyplot as plt

def plot_per_day(df, name):
    plt.style.use('seaborn')
    plt.figure(figsize=(30,10))
    scatterplot(data=df, x='ds', y='y', alpha=.4, color="grey", label="valor diário")
    lineplot(x=df['ds'], y=df['y'].rolling(30).mean(), label="Média movel: 1 mês")
    lineplot(x=df['ds'], y=df['y'].rolling(7).mean(), label="Média movel: 1 semana", color='goldenrod')
    # lineplot(x=df['ds'], y=df['y'].rolling(365).mean(), label="Média movel: 1 ano")
    # lineplot(x=df_covid2['ds'], y=df_covid2['y'],label="Covid")
    plt.title(name, fontdict={'fontsize':20})
    plt.show()
    
    
def plot_per_period(df, name):
    plt.style.use('seaborn')
    plt.figure(figsize=(30,10))
    scatterplot(data=df, x='ds', y='y', alpha=.4, color="grey", label="valor diário")
    lineplot(x=df['ds'], y=df['y'].rolling(120).mean(), label="Média movel: 1 mês")
    lineplot(x=df['ds'], y=df['y'].rolling(28).mean(), label="Média movel: 1 semana", color='goldenrod')
    # lineplot(x=df['ds'], y=df['y'].rolling(365).mean(), label="Média movel: 1 ano")
    # lineplot(x=df_covid2['ds'], y=df_covid2['y'],label="Covid")
    plt.title(name, fontdict={'fontsize':20})
    plt.show()
    
    
def plot_compare_results(df, column1, column2):
    plt.style.use('seaborn')
    df_ = df[['ds', 'y', column1, column2]].copy()
    df_[column1] = df_[column1].round(0).astype(int)
    df_[column2] = df_[column2].round(0).astype(int)
    fig_dims = (30,24)
    fig_, ax = plt.subplots(3,1, figsize=fig_dims)
    label1 = column1.split('_')[-1].capitalize()
    label2 = column2.split('_')[-1].capitalize()
    lineplot(x=df_['ds'], y=df_[column1], label=label1, ax=ax[0])
    lineplot(x=df_['ds'], y=df_[column2], label=label2, ax=ax[0])
    lineplot(x=df_['ds'], y=df_['y'], label="y", ax=ax[0])
    
    df_[f'abs_error_{label1}'] = abs(df_['y']-df_[column1])
    df_[f'abs_error_{label2}'] = abs(df_['y']-df_[column2])
    
    lineplot(x=df_['ds'], y=df_[f'abs_error_{label1}'], label=f"ERROR - {label1}", ax=ax[1])
    lineplot(x=df_['ds'], y=df_[f'abs_error_{label2}'], label=f"ERROR - {label2}", ax=ax[1])
    
    lineplot(x=df_['y'], y=df_[f'abs_error_{label1}'], label=f"ERROR - {label1}", ax=ax[2])
    lineplot(x=df_['y'], y=df_[f'abs_error_{label2}'], label=f"ERROR - {label2}", ax=ax[2])
    
    ax[0].set_ylabel('qtd')
    ax[1].set_ylabel('qtd')
    ax[2].set_ylabel('qtd')
    
    plt.show()