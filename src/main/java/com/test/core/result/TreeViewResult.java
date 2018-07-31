package com.test.core.result;
import java.util.List;

/**
 * 菜单树
 * 
 *
 */
public class TreeViewResult {

	private Long id;
	private String pid;
	private String text;
	private List<TreeViewResult> nodes;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getPid() {
		return pid;
	}

	public void setPid(String pid) {
		this.pid = pid;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public List<TreeViewResult> getNodes() {
		return nodes;
	}

	public void setNodes(List<TreeViewResult> nodes) {
		this.nodes = nodes;
	}

}
