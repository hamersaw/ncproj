package org.sustain.etl.ncproj;

public class DumpOperand {
    protected String shapeId; 
    protected int timeOffset;

    public DumpOperand(String shapeId, int timeOffset) {
        this.shapeId = shapeId;
        this.timeOffset = timeOffset;
    }

    public String getShapeId() {
        return this.shapeId;
    }

    public int getTimeOffset() {
        return this.timeOffset;
    }
}
