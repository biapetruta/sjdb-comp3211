### COMP3211 : Advanced Databases Coursework

This is a coursework in a 3rd year Database module. The target is to take queries in SQL like language and optimise them based on
the information available about the database entities.

The ```src/``` folder contains the source code for the main program as well as the tests.
  1. ```src/sjdb``` file contains Two important files
    1. ```Estimator.java```, which takes in a database logical query and estimates the total cost in terms of disk access
    2. ```Optimiser.java```, which takes in a database logical query and optmises it by pushing down _Select_, creating _Join_ and adding
    _Project_ operators.
  2. ```src/test``` folder contains JUnit code to test the Estimator. **WARNING**: The tests are **NOT** complete and **NOT** verified.
  It was only a start. You are highly advised to edit those as you need.
  
Other than that, the rest of the source files were prepared by Dr. Nicholas Gibbins at the University of Southampton.
