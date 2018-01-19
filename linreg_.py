# linreg.py
#
# Standalone Python/Spark program to perform linear regression.
# Performs linear regression by computing the summation form of the
# closed form expression for the ordinary least squares estimate of beta.
# 
# TODO: Write this.
# 
# Takes the yx file as input, where on each line y is the first element 
# and the remaining elements constitute the x.
#
# Usage: spark-submit linreg.py <inputdatafile>
# Example usage: spark-submit linreg.py yxlin.csv
#
#

import sys
import numpy as np
from numpy.linalg import inv
from pyspark import SparkContext


if __name__ == "__main__":
  if len(sys.argv) !=2:
    print >> sys.stderr, "Usage: linreg <datafile>"
    exit(-1)

  sc = SparkContext(appName="LinearRegression")

  # Input yx file has y_i as the first element of each line 
  # and the remaining elements constitute x_i
  yxinputFile = sc.textFile(sys.argv[1])

  
  #print "yxlength: ", yxlength

  # dummy floating point array for beta to illustrate desired output format
  
  #
  # Add your code here to compute the array of 
  # linear regression coefficients beta.
  # You may also modify the above code.
  #
  # this function returns the dot productof xi and xiTranspose
  def firstCalculation(lineData):
      lineData_a = lineData.split(',')#splits the lineData using , as seperator
      lineData_a[0] = 1  #This replaces yi in lineData_a array with 1
      xi = np.array([lineData_a], dtype=float) #chnges all the values to float datatype
      xiTranspose = xi.transpose() #calculates transpose of the xi matrix
      value = np.dot(xiTranspose, xi) #stores the dot product of xi and xiTranspose
      return value
  # this function returns dot product of xiTranspose and y
  def secondCalculation(lineData):
      lineData_b = lineData.split(',')#splits the lineData using , as seperator 
      y = lineData_b[0]#takes the first element of array
      y = float(y)#changes it to float 
      lineData_b[0] = 1 #replaces yi with 1
      xi = np.array([lineData_b], dtype=float)#changes all values to float datatype
      xiTranspose = xi.transpose()#calculates transpose of xi matrix
      value_1 = np.dot(xiTranspose,y)#stores the dot product of xiTranspose and yi
      return value_1

xi_xiTransProduct = yxinputFile.map(firstCalculation) #sends the yxinputFIle through a map
	
matrix_xi_xiTranspose = xi_xiTransProduct.reduce(lambda p,q: np.add(p,q))#total of xi_xiTranspose	
	
invMatrix_xi_xiTranspose = inv(matrix_xi_xiTranspose)#inverse of above matrix
	
xi_yiProduct = yxinputFile.map(secondCalculation)
	
matrix_xi_yi = xi_yiProduct.reduce(lambda p,q: np.add(p,q))#total of xi_yimatrix	
	
beta = np.dot(invMatrix_xi_xiTranspose,matrix_xi_yi)#dot product of xiyi and xixitranspose
	

  # print the linear regression coefficients in desired output format
print "beta: "
for coeff in beta:
     print coeff

sc.stop()
