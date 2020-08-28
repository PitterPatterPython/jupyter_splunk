# jupyter_splunk
A module to help interaction with Jupyter Notebooks and Splunk

------
This is a python module that helps to connect Jupyter Notebooks to various datasets. 
It's based on (and requires) https://github.com/JohnOmernik/jupyter_integration_base 



## Initialization 
----

### Example Inits

#### Embedded mode using qgrid
```
from splunk_core import Splunk
ipy = get_ipython()
Splunk = Splunk(ipy, debug=False, pd_display_grid="qgrid")
ipy.register_magics(Splunk)
```

