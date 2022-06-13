
from faker import Faker 
from random import randint 
import pandas as pd 
import numpy as np

from Recommender_System import DataPreparation


def generateur_modulaire(a,b,x0,nbre):
  """
  
   Generate a sequence of numbers that are differents one from another
   
   Parameters 
   -----------
  a,b,x0: int 
    initialization of our generator
  nbre : int 
    number of elements of the sequence generated   

  RETURN
  -----------
     list of int   

  """
  seq=[]
  x=x0
  seq+=[x]
  for i in range (1,nbre):
      seq+=[(a*x+b)]
      x=seq[i]
  return seq







#import excel file :job_table 

DF_job=pd.read_csv('marketing_sample_for_naukri_com-jobs.csv',encoding='utf-8-sig')

  #rename 'job title' column into 'title' 
DF_job.rename(columns={'Uniq Id':'ID'},inplace= True)

df_job=DataPreparation.data_preparation()
df_job=df_job._assign_unique_id(DF_job,'ID')



df_job = df_job.loc[df_job['ID'] <= 1500 ]
df_job.to_csv('job.csv',index=False,encoding='utf-8-sig')



def extract_user_interests_from_job_table(df_job):
  """

  Retreive users interests from job table

  Parameters
  -----------
  df_job :Pandas DataFrame
    the Job dataframe

  Returns
  ---------
   list of user intersts   
   
  """

  interests_list=[]

  interests_Series=df_job['Key Skills']

  for e in interests_Series :
    if e not in interests_list :
        interests_list +=[e]
  
  return interests_list






def generate_user_table(nbre_row, df_job):
  """

    Generate user dataframe

    Parameters
    ----------
    nbre_row :int 
      number of rows in the generated dataFrame of user
      we choosed to make it equal to the number of jobs

    df_job : Pandas dataFrame
      Pandas DataFrame of Job table

    RETURN
    -------
    pandas DataFrame
  """

  fake=Faker('en_US')

  data_user={'id':[],'name':[],'email':[],'password':[],'image':[],'tel':[],'interests':[]}
  nbre_data = nbre_row 

  a,b,x0=1,1,1
  id_list=generateur_modulaire(a,b,x0,nbre_data)

  
  user_interest_list=extract_user_interests_from_job_table(df_job)

  

  for i in range(nbre_data):

    data_user['id'] += [id_list[i]]
    
    data_user['name']+=[fake.name()]
    data_user['email']+=[fake.email()]
    data_user['password']+=["$2y$10$gaKPBXfyOviXFsbJhMuL4ejKYK/R8mwIQWzqCF.70TT.uPIcJ3M3C"]
    data_user['image']+=["image_defaut1234.jpg"]
    data_user['tel']+=[fake.phone_number()]
    a=randint(0,len(user_interest_list)-1)
    data_user['interests']+=[user_interest_list[a]]

  user_df=pd.DataFrame( data_user , index = np.array(range(nbre_data) ,dtype=int)  )

  return user_df

nber_row_in_df_job = len(df_job)


df_user=generate_user_table(nber_row_in_df_job, df_job)

df_user.to_csv('user.csv',index=False,encoding='utf-8-sig')




def generate_click_table(df_user,df_job):
  """

  Create a dataframe called 'click'

  Parameters
  -----------
  df_user : pandas DataFrame
    the user dataframe
  df_job : pandas DataFrame
    the job dataframe

  Returns
  ----------
  pandas DataFrame
    the click dataframe

  """
  
  data_click={'click_id':[],'click_author_id':[],'click_job_id':[]}
  nbre_data =len( df_user )
  nbre_user_data =len( df_user )
  nbre_job_data =len( df_job )
  a,b,x0=1,1,1
  nbre_min_data=min(nbre_user_data,nbre_job_data)
  nbre_max_data=max(nbre_user_data,nbre_job_data)

  id_list=generateur_modulaire(a,b,x0,nbre_min_data)
  for i in  range( nbre_min_data ):
   

    data_click['click_id'] += [id_list[i]]
    a=randint(0,nbre_min_data-1)
    data_click['click_author_id'] += [ id_list[a] ]
    b=randint(0,nbre_min_data-1)
    data_click['click_job_id'] += [ id_list[b] ]



  df_click=pd.DataFrame(data_click , index=np.array(range(nbre_min_data), dtype=int) )
  return df_click


df_click=generate_click_table(df_user,df_job)



df_click.to_csv('click.csv',index=False,encoding='utf-8-sig')
df_click_2=df_click
df_click_2.to_csv('click_2.csv',encoding='utf-8-sig')



def genearate_score_table(df_user):
   """

   Create a dataframe for  the table called :'score'.

   Parameters:
   -----------
   df_user : pandas DataFrame
   the user dataframe
  
   Returns:
   ---------
   pandas DataFrame 
   a pandas dataframe of 'score'
      
   """
  
  
   
   data_score={'score_score_id':[] , 'score_user_id':[] ,'score_job_id':[] , 'score_score_value' :[]}
   nbre_data =len( df_user )

   nbre_user_data =len( df_user )
   nbre_job_data =len( df_job )

   a,b,x0=1,1,1
   nbre_max_data=max(nbre_user_data,nbre_job_data)
   nbre_min_data=min(nbre_user_data,nbre_job_data)
   id_list=generateur_modulaire(a,b,x0,nbre_min_data)
   

   

   for i in  range( nbre_min_data ):
    data_score['score_score_id'] += [id_list[i]]

    a=randint(0,nbre_min_data-1)
    
    data_score['score_job_id'] += [ int( df_click_2['click_job_id'][i] ) ]
    
    data_score['score_user_id'] += [ int( df_click_2['click_author_id'][i] ) ]

    data_score['score_score_value']+=[randint(0,5)]


  
   df_score =pd.DataFrame(data_score , index=np.array(range(nbre_min_data), dtype=int) )
   return df_score

df_score =  genearate_score_table(df_user)  



df_score.to_csv('score.csv',index=False,encoding='utf-8-sig')
