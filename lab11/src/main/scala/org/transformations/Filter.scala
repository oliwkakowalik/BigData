package org.transformations

import org.caseClass.Iris

class Filter  extends java.io.Serializable{

  def sepal_w_gt(iris: Iris, value: Double): Boolean = {
    return iris.sepal_width > value;
  }

}
