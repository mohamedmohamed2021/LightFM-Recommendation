from turtle import right
import numpy as np


class data_preparation :


    """
    Clean and process data 
    
    This class process data in a way that will be useful for building
    lightFM dataset

    """

    

    def __init__(self):
        #just passing without executing anything !
        pass
     
     
    def _assign_unique_id(self ,data ,id_col_name ):


        """
    
        Generate unique integer id for users,jobs and cliks 

        Parameters
        ----------
        data: Dataframe
            Pandas Dataframe for Users , jobs or clicks. 
        id_col_name : String 
            New integer id's column name.

        Returns
        -------
        Dataframe
            Updated dataframe containing new id column
        """ 

        df_dimention=len(data)
        new_ids_list=np.arange(df_dimention)

        

        data[str(id_col_name)] = new_ids_list  
        return data



    def _dropna(self, data, column, axis):

          """ Drop null values from specific column"""

          return data.dropna(column, axis=axis)



    def _merge_data(self,left_data,left_key,right_data,right_key , how ):
            """

            Merging two dataframe

            Parameters 
            -----------
            left_data: Dataframe
                Left side dataframe for merge
            left_key: String
                Left Dataframe merge key
            right_data: Dataframe
                Right side dataframe for merge
            right_key: String
                Right Dataframe merge key
            how: String
                Method of merge (inner, left, right, outer)

            Returns 
            -----------
            Dataframe
               A new dataframe merging left and right dataframe    
             
            """
            return left_data.merge(
            right_data,
            how=how,
            left_on=left_key,
            right_on=right_key)
 
    def _group_tags(self, data, group_by, tag_column):

          """ Group multiple tags into single rows sepearated by comma """
          return data.groupby(
            [group_by])[tag_column].apply(
            ','.join).reset_index()   

    def merge_csv_datasets(
        self ,
        user,job,click,score
    ):        

        """

        This function merge all the necessary  dataset in a defined way 

        Parameters 
        -----------
        user,job,click,score :Pandas DataFrames
            datasets of our project
        
        Returns
        ----------
        merge : Pandas DataFrame
            a new dataframe after merging clicks ,jobs,users and scores 

        """
        
        merge =self._merge_data(
            left_data=click,
            left_key='click_job_id',
            right_data=job,
            right_key='ID',
            how='inner')

           
        
        merge=self._merge_data(
            left_data=merge,
            left_key='click_author_id',
            right_data=user,
            right_key='id',
            how='inner'
        )   


        merge=self._merge_data(
            left_data=merge,
            left_key=('click_author_id','click_job_id',),
            right_data=score,
            right_key=('score_user_id','score_job_id'),
            how='inner'
        )
        
        
        return merge
    

    def _drop_duplicates_tags(self, data, col_name):

        """drop duplicates tags from each row"""
        return (
            data[col_name].str.split(
                ',').apply(set).str.join(','))

    def _merge_user_previous_clicked_jobs(self, user, merge):



        """
        Determine the previous jobs clicked by a user.
        Then merge these jobs' features with the user features.
        In the end ,we will add an additional column in the user dataframe 
        called :'user_all_features'.
        the data in this columns will be like that :
         (user_ferature , job1_feature, job2_feature ,...,jobm_feature)
        where : job1,job2,....: are the previous jobs that the user have already visited/clicke them .
        
        Parameters
        -----------
        user : Pandas DataFrame
           the user dataset
        merge : Pandas DataFrame
           the table that merges all datsets,
           it merges :user,job,clickand score .

        Returns
        -------
        user DataFrame which was updated ,by adding the 'user_all_features' column.

        """

        user_prev_click_job=(merge[ ['id','Job Title'] ])

        #drop null values
        user_prev_click_job=user_prev_click_job.dropna()
        
        # because users can click to multiple jobs,
        # we group all of job's titles of each user into single row
        user_prev_click_job = self._group_tags(
            data=user_prev_click_job,
            group_by='id',
            tag_column='Job Title')

          
        
        # drop duplicates job titles from each user rows
        user_prev_click_job['Job Title'] = \
        self._drop_duplicates_tags(
            user_prev_click_job, 'Job Title')
        #user_prev_click_job.to_csv('user_prev_click_job_2.csv',index=False,encoding='utf-8-sig')    
        
        # finally merge the dataframe with user dataframe
        user = self._merge_data(
            left_data=user,
            left_key='id',
            right_data=user_prev_click_job,
            right_key='id',
            how='left')
        #user.to_csv('user_3.csv',index=False,encoding='utf-8-sig')    

        # join users tags (= user interests ) and their clicked  job titles 
        # we replace nan values with ""
        user['user_all_features'] = (
            user[['interests',
                           'Job Title']].apply(
                lambda x: ','.join(x.dropna()),
                axis=1)) 
        #user.to_csv('user_4.csv',index=False,encoding='utf-8-sig')        

        return user   


    def prepare (
            self,
            user ,
            job,
            click,
            score
        ):
            
            """
            This function cleans and process our project datasets 
            In fact , it will use all the above functions in this Class
            
            Parameters 
            -------------
            user,job,click, score :Pandas DataFrame
                our project's datasets

            Returns     
            -----------
            df_user, df_job, df_merge :Pandas DataFrame


            """





            #assign unique integer id 
            user = self._assign_unique_id(user,'id')
            job  = self._assign_unique_id(job,'ID')
            click= self._assign_unique_id(click,'click_id')
            score=self._assign_unique_id(score,'score_score_id')
            
            

            
            #handle null values in score dataset
            score['score_score_value']=score['score_score_value'].fillna(0)
            score['score_score_value']=score['score_score_value'].astype(int)

            #handle null values in job names
            job['Job Title']=job['Job Title'].fillna('No job title')

            #remove duplicate title from each job 
            job['Job Title']=job['Job Title'].str.split(
            ',').apply(set).str.join(',')

        


            #merge necessary data
            df_user=user 
            df_job =job
            
            
            df_merge =self.merge_csv_datasets(user,job,click,score)





            #Generate some features to calculate weight
            #this will be user in the interacy=tion matrix 
            df_merge['num_clicks_per_job']=df_merge.groupby(['ID'])['click_id'].transform('count')
            


            
            # merge user previous cliked job features(=title) with user features (=interests)
            df_user=self._merge_user_previous_clicked_jobs(user,df_merge)
            

            #handle Nan values :replace them with 'No information' 
            df_user['user_all_features']=df_user['user_all_features'].fillna('No information')


            #remove duplicate interests from each user
            df_user['user_all_features'] = \
            df_user['user_all_features'].str.split(
            ',').apply(set).str.join(',')
            



            # remove some null values from df_merge
            df_merge['num_clicks_per_job']  = \
            df_merge['num_clicks_per_job'].fillna(0)

            
            df_user.to_excel("user_prepared.xlsx",encoding='utf-8-sig')
            df_job.to_excel("job_prepared.xlsx",encoding='utf-8-sig')
            score.to_excel("score_prepared.xlsx",encoding='utf-8-sig')
            df_merge.to_excel("merge_prepared.xlsx",encoding='utf-8-sig')



            return df_user, df_job, df_merge



          


            




 