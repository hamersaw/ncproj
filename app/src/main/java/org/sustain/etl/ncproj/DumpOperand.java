package org.sustain.etl.ncproj;

public class DumpOperand {
    protected String shapeId; 
    protected int timeOffset;
    protected int timeIndex;

    public DumpOperand(String shapeId, int timeOffset, int timeIndex) {
        this.shapeId = shapeId;
        this.timeOffset = timeOffset;
        this.timeIndex = timeIndex;
    }

    public String getShapeId() {
        return this.shapeId;
    }

    public int getTimeOffset() {
        return this.timeOffset;
    }

    public int getTimeIndex() {
        return this.timeIndex;
    }
}
