package com.cbp.bean;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.io.Serializable;
import java.util.Set;


@ApiModel(value = "实体目录实体", description = "实体目录实体请求类")
public class AddEntityCatalogReq implements Serializable {
    private static final long serialVersionUID = 1L;

    @ApiModelProperty(value = "实体guid")
    private String guid;

    @ApiModelProperty(value = "目录名称")
    private String name;

    @ApiModelProperty(value = "目录全路径")
    private String path;

    @ApiModelProperty(value = "目录级别")
    private String level;
    @ApiModelProperty(value = "英文名称")
    private String nameEng;

    @ApiModelProperty(value = "数据组织")
    private String dataDepartment;
    @ApiModelProperty(value = "数据owner人员")
    private String dataOwner;
    @ApiModelProperty(value = "编码")
    private String code;
    @ApiModelProperty(value = "别名")
    private String alias;
    @ApiModelProperty(value = "唯一标识名称")
    private String qualifiedName;
    @ApiModelProperty(value = "创建人")
    private String createdBy;
    @ApiModelProperty(value = "更新人")
    private String updatedBy;
    @ApiModelProperty(value = "排序")
    private String order;
    @ApiModelProperty(value = "描述")
    private String desc;
    @ApiModelProperty(value = "父guid")
    private String parentGuid;

    @ApiModelProperty(value = "子guid")
    private Set<String> childs;

}

