###########################################################
# Import some in-built python modules                     #
###########################################################

import numpy as np
import pandas as pd

###########################################################
# Import our modules                                      #
###########################################################

from Recommender_System import Building_Data_for_LightFM
from Recommender_System import DataPreparation
from Recommender_System import Training_Model
from Recommender_System import Recommendation


###########################################################
#  import LightFM accuracy libraries                        #
###########################################################

from lightfm.data import Dataset
from lightfm import LightFM
from lightfm import cross_validation
from lightfm.evaluation import precision_at_k
from lightfm.evaluation import auc_score


class Main():

  def __init__(self,user_id):
      self.id_user=user_id
      
      pass







  def get_recommended_jobs(self):




   ###########################################################
   # Import our project's datasets                           #
   ###########################################################




   df_user=pd.read_csv('user.csv',encoding='utf-8-sig')
   df_job=pd.read_csv('job.csv',encoding='utf-8-sig')
   df_score=pd.read_csv('score.csv',encoding='utf-8-sig')
   df_click=pd.read_csv('click.csv',encoding='utf-8-sig')









   ###########################################################
   #   instanciate classes                                   #
   ###########################################################

   our_project_data_prep = DataPreparation.data_preparation()
   light_fm_data_prep = Building_Data_for_LightFM.LightFMDataPrep()
   train_lightfm = Training_Model.TrainLightFM()


   ###########################################################
   #   Process row data                                      #
   ###########################################################

   

   df_user_p, df_job_p, df_merge_p = \
   our_project_data_prep.prepare(df_user,
    df_job,
    df_click,
    df_score
    )

   

   ###########################################################
   #   Prepare data for lightFm                              #
   ###########################################################


   interactions,\
    weights,job_features,user_features = \
   light_fm_data_prep.fit(
    df_job_p,df_user_p, df_merge_p)

   

   ###########################################################
   #   Build and train our model                             #
   ###########################################################
 

   model = train_lightfm.fit(interactions,
                          weights,
                          job_features,
                          user_features)




   ###########################################################
   #   Make real recommendation                              #
   ###########################################################


   # instanciate our recommender class


   lightfm_recommendations = Recommendation.LightFMRecommendations(
    model,
    user_features,job_features,
    df_job_p, df_user_p, df_merge_p)


   # let's see  what our model predict for user id 3
   
   self.lightfm_model=model
   self.interactions_matrix=interactions
   self.question_features=job_features
   self.professional_features=user_features

  #simple testing 
   df_recommended_jobs=lightfm_recommendations.recommend_by_user_id_general(user_id=self.id_user,num_prediction =4)

   df_recommended_jobs.to_excel("df_recommended_jobs.xlsx",encoding='utf-8-sig')

   return df_recommended_jobs













  def calculate_auc_score(self): 
    
    """
    Measure the ROC AUC metric for a model. 
    A perfect score is 1.0.

    Parameters
    ----------
    lightfm_model: LightFM model 
        A fitted lightfm model 
    interactions_matrix : 
        A lightfm interactions matrix 
    question_features, professional_features: 
        Lightfm features 
        
    Returns
    -------
    String containing AUC score 
    """
    
    lightfm_model=self.lightfm_model
    interactions_matrix=self.interactions_matrix
    question_features=self.question_features
    professional_features=self.professional_features


    score = auc_score( 
        lightfm_model, interactions_matrix, 
        item_features=question_features, 
        user_features=professional_features, 
        num_threads=4).mean()
    return score










test=Main(350)
df_recommended_jobs=test.get_recommended_jobs()
model_accuracy=test.calculate_auc_score()

print(model_accuracy)