import pandas as pd
import numpy as np
# all lightfm imports 
from lightfm.data import Dataset
from lightfm import LightFM
from lightfm import cross_validation
from lightfm.evaluation import precision_at_k
from lightfm.evaluation import auc_score

class TrainLightFM :
    """
    This class allows us to train LightFM model in a simple way

    """

    def __init__(self):
        pass

    def train_test_split(self, interactions, weights):
        train_interactions, test_interactions = \
        cross_validation.random_train_test_split(
            interactions, 
            random_state=np.random.RandomState(2019))
        
        train_weights, test_weights = \
        cross_validation.random_train_test_split(
            weights, 
            random_state=np.random.RandomState(2019))
        return train_interactions,test_interactions, train_weights, test_weights
    
    def fit(self, interactions, weights,
            job_features, user_features,
            cross_validation=True,no_components=150,
            learning_rate=0.90,
            loss='warp',
            random_state=2019,
            verbose=False,
            num_threads=7, epochs=100):
        ################################
        # Model building part
        ################################

        # define lightfm model by specifying hyper-parametre
        # then fit the model with ineteractions matrix,
        # item and user features
        
        model = LightFM(
            no_components,
            learning_rate,
            loss=loss,
            random_state=random_state)
        model.fit(
            interactions,
            item_features=job_features,
            user_features=user_features, sample_weight=weights,
            epochs=epochs, num_threads=num_threads, verbose=verbose)
        
        return model