package edu.ecnu.touchstone.datatype;

import java.io.Serializable;

public interface TSDataTypeInfo extends Serializable {

    Object geneData();

    double getMinValue();

    double getMaxValue();
}
