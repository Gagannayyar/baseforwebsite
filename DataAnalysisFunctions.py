"""
--------------------------------------------------------------------------------
SOME HELPFUL FUNCTIONS FOR DATA ANALYSIS TO MAKE LIFE A BIT EASIER
--------------------------------------------------------------------------------
"""


def plot_numeric(df):
    """
    Plot all the numeric columns in the dataset
    """
    import matplotlib.pyplot as plt #Library import
    df_columns = list(df.select_dtypes(include=np.number).columns) #Numerical Columns
    for i in df_columns:
        df[i].plot(figsize=(20,5),title=i)
        plt.show() #Remove this if you want to all the plots in the same figure
        
        
        
def qqplot(df):
    """
    See the qqplots of numerical columns 
    to check if they are normally distributed or not
    """
    #Libraries
    import scipy.stats
    import pylab
    
    #getting the numerical columns
    df_columns = list(df.select_dtypes(include=np.number).columns)
    
    #QQplotting
    for i in df_columns:
        scipy.stats.probplot(df[i], plot=pylab)
        pylab.title(i.upper())
        pylab.show()
    
def heat_map(df):
    
    """
    Get the heatmap from a dataframe
    """
    import seaborn as sn # required library
    import pandas as pd
    CorrMatrix = df.corr()
    return sn.heatmap(corrMatrix, annot=True)
    
    
    
def discrete_count(df, unique_count=20):
    """
    Select all the object/ string columns in the 
    dataframe with discrete values (<20 unique values default)
    and gives the count of the discreate 
    values in them
    """
    
    #selecting the string columns
    
    df_columns = list(df.select_dtypes(exclude=['int64', 'float64', 'bool', 'datetime']).columns)
    
    for i in df_columns:
        if len(df[i].unique()) < unique_count:
            print(f'-----{i}-----')
            print(df[i].value_counts())
            
    
    