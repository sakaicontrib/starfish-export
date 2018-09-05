package org.sakaiproject.starfish.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@AllArgsConstructor
@ToString(includeFieldNames=true)
public class StarfishScore {
	public static final String[] HEADER = {"gradebook_item_integration_id","course_section_integration_id","user_integration_id","points","points_possible_override","graded_ts"};

	@Getter @Setter
	private String gradebook_item_integration_id;

	@Getter @Setter
	private String course_section_integration_id;

	@Getter @Setter
	private String user_integration_id;

	@Getter @Setter
	private String points;

	@Getter @Setter
	private String points_possible_override;

	@Getter @Setter
	private String graded_ts;
}
