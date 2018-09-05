package org.sakaiproject.starfish.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@AllArgsConstructor
@ToString(includeFieldNames=true)
public class StarfishAssessment {
	public static final String[] HEADER = {"integration_id","course_section_integration_id","title","description","due_dt","points_possible","scorable_ind","calculated_ind","external_grade_ind"};

	@Getter @Setter
	private String integration_id;

	@Getter @Setter
	private String course_section_integration_id;

	@Getter @Setter
	private String title;

	@Getter @Setter
	private String description;

	@Getter @Setter
	private String due_dt;

	@Getter @Setter
	private String points_possible;

	@Getter @Setter
	private int scorable_ind;
	
	@Getter @Setter
	private int calculated_ind;

	@Getter @Setter
	private int external_grade_ind;
}
