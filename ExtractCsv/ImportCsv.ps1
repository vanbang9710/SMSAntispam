conda create -n mysql -c conda-forge ipykernel pandas mysql-connector-python
conda activate mysql
conda install -n mysql -c conda-forge sqlalchemy
# conda install -n mysql -c conda-forge gym=0.24.0
# conda install -n mysql -c "conda-forge/label/broken" gym=0.24.0
pip install tensorflow
pip install gym==0.24.0
# pip install tensorflow gym --upgrade
