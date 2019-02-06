#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# https://www.kaggle.com/puneetgrover/speed-up-your-algorithms-dask
# dask_kaggle_Regression


# In[1]:


from dask_ml.datasets import make_regression
import dask.dataframe as dd

X, y = make_regression(n_samples=1e6, chunks=50000)


# In[2]:


df = dd.from_dask_array(X)
df.head()


# In[3]:


from dask_ml.model_selection import train_test_split, GridSearchCV

xtr, ytr, xval, yval = train_test_split(X, y)

 


# In[ ]:




