package com.thit.tibdm.parse;


import java.util.List;

/**
 * 解析规则
 */
public class ParseRule {
    /**
     * 基础变量
     */
    private List<Variable> variables;
    /**
     * 虚拟变量
     */
    private List<VirtualVariable> virtualVariables;
    /**
     * 异常变量列表
     */
    private List<WarnVariable> warnVaribales;
    /**
     * 特殊变量列表
     */
    private List<String> specialVariable;
    /**
     * 0表示单包发送，1表示分包发送
     */
    private int sendMode;

    public ParseRule() {
    }

    public ParseRule(List<Variable> variable, List<VirtualVariable> virtualVariable) {
        this.variables = variable;
        this.virtualVariables = virtualVariable;
    }

    public ParseRule(List<Variable> variables,
                     List<VirtualVariable> virtualVariables,
                     List<WarnVariable> warnVaribales,
                     List<String> specialVariable) {
        this.variables = variables;
        this.virtualVariables = virtualVariables;
        this.warnVaribales = warnVaribales;
        this.specialVariable = specialVariable;
    }


    public ParseRule(List<Variable> variables,
                     List<VirtualVariable> virtualVariables,
                     List<WarnVariable> warnVaribales,
                     List<String> specialVariable,
                     int sendMode) {
        this.variables = variables;
        this.virtualVariables = virtualVariables;
        this.warnVaribales = warnVaribales;
        this.specialVariable = specialVariable;
        this.sendMode = sendMode;
    }

    public List<String> getSpecialVariable() {
        return specialVariable;
    }

    public void setSpecialVariable(List<String> specialVariable) {
        this.specialVariable = specialVariable;
    }

    @Override
    public String toString() {
        return "ParseRule{" +
                "variables=" + variables.toString() +
                ", virtualVariables=" + virtualVariables.toString() +
                '}';
    }

    public List<Variable> getVariables() {
        return variables;
    }

    public void setVariables(List<Variable> variables) {
        this.variables = variables;
    }

    public List<WarnVariable> getWarnVaribales() {
        return warnVaribales;
    }

    public void setWarnVaribales(List<WarnVariable> warnVaribales) {
        this.warnVaribales = warnVaribales;
    }

    public List<VirtualVariable> getVirtualVariables() {
        return virtualVariables;
    }

    public void setVirtualVariables(List<VirtualVariable> virtualVariables) {
        this.virtualVariables = virtualVariables;
    }

    public int getSendMode() {
        return sendMode;
    }

    public void setSendMode(int sendMode) {
        this.sendMode = sendMode;
    }
}
