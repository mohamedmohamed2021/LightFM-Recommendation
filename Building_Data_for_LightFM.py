import pandas as pd
# all lightfm imports 
from lightfm.data import Dataset
from lightfm import LightFM
from lightfm import cross_validation
from lightfm.evaluation import precision_at_k
from lightfm.evaluation import auc_score

class LightFMDataPrep :
    
  """
   
   Since LightFm library accepts data in avery specific way , it is mandatory to
   adjust our data in order to be ready for feed by LightFM .
   Thus this class make our data compatible with the one of LightFM

  """

  def __init__(self):
      pass

  def create_features(self, dataframe, features_name, id_col_name):
        """
        Generate features that will be ready for feeding into lightfm

        Parameters
        ----------
        dataframe: Dataframe
            Pandas Dataframe which contains features
        features_name : List
            List of feature columns name avaiable in dataframe
        id_col_name: String
            Column name which contains id of the job or
            click that the features will map to.
            There are two possible values for this variable.
            1. job_id_num
            2. user_id_num

        Returns
        -------
        Pandas Series
            A pandas series containing process features
            that are ready for feed into lightfm.
            The format of each value
            will be (user_id, ['feature_1', 'feature_2', 'feature_3'])
            Ex. -> (1, ['military', 'army', '5'])
        """

        features = dataframe[features_name].apply(
            lambda x: ','.join(x.map(str)), axis=1)
        features = features.str.split(',')
        features = list(zip(dataframe[id_col_name], features))
        return features
    

  def generate_feature_list(self, dataframe, features_name):
        """
        Generate features list for mapping 

        Parameters
        ----------
        dataframe: Dataframe
            Pandas Dataframe for user ,job ,click . 
        features_name : List
            List of feature columns name avaiable in dataframe. 

        Returns
        -------
        List of all features for mapping 
        """
        features = dataframe[features_name].apply(
            lambda x: ','.join(x.map(str)), axis=1)
        features = features.str.split(',')
        features = features.apply(pd.Series).stack().reset_index(drop=True)
        return features

  def create_data(self, job , user , merge):
     """

     Create feautes  ready to be feeded into LightFm library 

     Parameters
     -----------
     job,user,merge : Pandas DataFrame
       our datasets 

     Returns 
     ----------
     job_feature_list,user_feature_list : Pandas Series 
        pandas series which contain a list of features 
     user,job,merge: Pandas Dataframe 
        our dataset which was updated:we've added additional columns for features
      

     """

     job_feature_list = self.generate_feature_list(
            job,
            ['Job Title'])

     user_feature_list = self.generate_feature_list(
            user,
            ['interests'])
        
     merge['total_weights'] = 1 / (
            merge['num_clicks_per_job'])
        
    # creating features for feeding into lightfm 
     job['job_features'] = self.create_features(
            job, ['Job Title'], 
            'ID')

     user['user_features'] = self.create_features(
            user,
            ['interests'],
            'id')
        
     return job_feature_list,\
    user_feature_list,user,job,merge  

  def fit(self, job, user , merge):
        """
        
        This function build data to be compatible with LighFm library

        Parameters
        ------------
        job,user,merge :Pandas DataFrame
         our datasets 

        Returns 
        -----------
        interactions,weights,job_features,user_features :
            LightFm arguments


        """
        
        ########################
        # Dataset building for lightfm
        ########################
        job_feature_list, \
        user_feature_list,\
        user,job,merge = \
        self.create_data(job,user, merge)
        
        job.to_excel("job_created.xlsx",encoding='utf-8-sig')
        user.to_excel("user_created.xlsx",encoding='utf-8-sig')
        merge.to_excel("merge_created.xlsx",encoding='utf-8-sig')
        job_feature_list.to_csv("job_feature_list.csv",encoding='utf-8-sig')
        user_feature_list.to_csv("user_feature_list.csv",encoding='utf-8-sig')

        # define our dataset variable
        # then we feed unique professionals and questions ids
        # and item and professional feature list
        # this will create lightfm internel mapping
        dataset = Dataset()
        

        dataset.fit(
            set(user['id']), 
            set(job['ID']),
            item_features=job_feature_list, 
            user_features=user_feature_list)


        # now we are building interactions
        # matrix between professionals and quesitons
        # we are passing professional and questions id as a tuple
        # e.g -> pd.Series((pro_id, question_id), (pro_id, questin_id))
        # then we use lightfm build in method for building interactions matrix
        merge['author_job_id_tuple'] = list(zip(
            merge.id,
            merge.ID,
            merge.total_weights))

        interactions, weights = dataset.build_interactions(
            merge['author_job_id_tuple'])

        



        # now we are building our questions and
        # professionals features
        # in a way that lightfm understand.
        # we are using lightfm build in method for building
        # questions and professionals features 
        job_features = dataset.build_item_features(
            job['job_features'])

        

        user_features = dataset.build_user_features(
            user['user_features'])
        
        return interactions,\
    weights,job_features,user_features

      