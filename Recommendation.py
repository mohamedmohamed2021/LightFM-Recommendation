import pandas as pd 
import numpy as np
from scipy import *
# all lightfm imports 
from lightfm.data import Dataset
from lightfm import LightFM
from lightfm import cross_validation
from lightfm.evaluation import precision_at_k
from lightfm.evaluation import auc_score


class LightFMRecommendations :
    """
    This class allows us to make predictions given the model and user ids

    """

    def __init__(self, lightfm_model,
                 users_features,
                 jobs_features,
                 job,user,merge):
                 
        """
        Parameters
        -----------
        users_features,jobs_features :LightFm features 
            these are the features recognized by LightFM library 
                 

        """

        self.model = lightfm_model
        self.users_features =users_features
        self.jobs_features = jobs_features
        self.job = job
        self.user = user
        self.merge = merge

    def previous_cliked_jobs(self, users_id_num):


        """

        Find previous jobs that were clicked by the given users_id list 
        
        """

        previous_jobs_id_num = (
            self.merge.loc[\
                self.merge['id'] == \
                users_id_num]['ID']).copy()
                


       
        
        previous_clicked_jobs = self.job.loc[\
            self.job['ID'].isin(
            previous_jobs_id_num)].copy()


        


       

             
        return previous_clicked_jobs    

    
    def _filter_job_by_user(self, users_id):


        """
        Drop jobs that users have already clicked.
        Then returns jobs that have never been clicked by the user. 
        This jobs will be the subject of our prediction later on .

        Returns
        ---------
        jobs_for_prediction :Pandas DataFrame
            the job tha the given users have never clicked

        """

        previous_clicked_jobs = \
        self.previous_cliked_jobs(users_id)
        
        discard_qu_id = \
        previous_clicked_jobs['ID'].values.tolist()
        
        
        
        jobs_for_prediction = \
        self.job.loc[~self.job['ID'].isin(discard_qu_id)]
        
     
        


        
        return jobs_for_prediction
    

    def recommend_by_user_id_general(self,
                                   user_id,
                                    num_prediction=8):


        """
        This function recommend jobs to the given user_id 

        Returns
        --------
        jobs_for_prediction :Pandas DataFrame
          the predicted jobs for the given user_id

        """                            



        jobs_for_prediction = self._filter_job_by_user(user_id)
       
       

        item_ids=jobs_for_prediction['ID'].values.tolist()
        
        
        nbre_items=len(item_ids)
        
        user_ids=np.ones(nbre_items,dtype=int)*user_id
      
      




        score = self.model.predict(
            user_ids,
            item_ids, 
            item_features=self.jobs_features,
            user_features=self.users_features,
            num_threads=3)
        

   
   

        jobs_for_prediction= jobs_for_prediction.assign( recommended_score =score)
        



        jobs_for_prediction = jobs_for_prediction.sort_values(
            by='recommended_score', ascending=False)[:num_prediction]
        
        
        
        


        return jobs_for_prediction

    