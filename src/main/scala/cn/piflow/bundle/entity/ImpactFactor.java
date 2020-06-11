package cn.piflow.bundle.entity;


import java.io.Serializable;

public class ImpactFactor implements Serializable {


    private double fullImpact;
    private double complexImpact;


    public double getFullImpact() {
        return fullImpact;
    }

    public void setFullImpact(double fullImpact) {
        this.fullImpact = fullImpact;
    }

    public double getComplexImpact() {
        return complexImpact;
    }

    public void setComplexImpact(double complexImpact) {
        this.complexImpact = complexImpact;
    }
}
