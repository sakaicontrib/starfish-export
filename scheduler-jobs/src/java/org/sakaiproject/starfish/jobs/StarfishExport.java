package org.sakaiproject.starfish.jobs;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.sakaiproject.authz.api.AuthzGroupService;
import org.sakaiproject.authz.api.GroupProvider;
import org.sakaiproject.authz.api.SecurityService;
import org.sakaiproject.component.api.ServerConfigurationService;
import org.sakaiproject.coursemanagement.api.AcademicSession;
import org.sakaiproject.coursemanagement.api.CourseManagementService;
import org.sakaiproject.coursemanagement.api.Membership;
import org.sakaiproject.event.api.EventTrackingService;
import org.sakaiproject.event.api.UsageSessionService;
import org.sakaiproject.exception.IdUnusedException;
import org.sakaiproject.service.gradebook.shared.Assignment;
import org.sakaiproject.service.gradebook.shared.GradeDefinition;
import org.sakaiproject.service.gradebook.shared.GradebookNotFoundException;
import org.sakaiproject.service.gradebook.shared.GradebookService;
import org.sakaiproject.site.api.Site;
import org.sakaiproject.site.api.SiteService;
import org.sakaiproject.site.api.SiteService.SelectionType;
import org.sakaiproject.site.api.SiteService.SortType;
import org.sakaiproject.starfish.model.StarfishAssessment;
import org.sakaiproject.starfish.model.StarfishScore;
import org.sakaiproject.tool.api.Session;
import org.sakaiproject.tool.api.SessionManager;
import org.sakaiproject.tool.gradebook.Gradebook;
import org.sakaiproject.user.api.User;
import org.sakaiproject.user.api.UserDirectoryService;

import com.opencsv.bean.ColumnPositionMappingStrategy;
import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;


/**
 * Job to export gradebook information to CSV for all students in all sites (optionally filtered by term)
 */
@Slf4j
public class StarfishExport implements Job {

	private final String JOB_NAME = "StarfishExport";
	private final static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
	private final static SimpleDateFormat tsFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private final static String nowTimestamp = tsFormatter.format(new Date());

	@Setter
	private SessionManager sessionManager;
	@Setter
	private UsageSessionService usageSessionService;
	@Setter
	private AuthzGroupService authzGroupService;
	@Setter
	private EventTrackingService eventTrackingService;
	@Setter
	private ServerConfigurationService serverConfigurationService;
	@Setter
	private SiteService siteService;
	@Setter
	private UserDirectoryService userDirectoryService;
	@Setter
	private GradebookService gradebookService;
	@Setter
	private GroupProvider groupProvider;
	@Setter
	private CourseManagementService courseManagementService;
	@Setter
	private SecurityService securityService;

	public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
		
		log.info(JOB_NAME + " started.");

		//get admin session
		establishSession(JOB_NAME);

		//get all sites that match the criteria
		String[] termEids = serverConfigurationService.getStrings("starfish.export.term");
		if (termEids == null || termEids.length < 1) {
			termEids = getCurrentTerms();
		}
		
		String fileSep = StringUtils.endsWith(getOutputPath(), File.separator) ? "" : File.separator;
		Path assessmentFile = Paths.get(getOutputPath() + fileSep + "assessments.txt");
		Path scoreFile = Paths.get(getOutputPath() + fileSep + "scores.txt");
	
		//delete existing file so we know the data is current
		if(deleteFile(assessmentFile)) {
			log.debug("New file: " + assessmentFile);
		}
		if(deleteFile(scoreFile)) {
			log.debug("New file: " + assessmentFile);
		}

		ColumnPositionMappingStrategy<StarfishAssessment> assessmentMappingStrategy = new StarfishAssessmentMappingStrategy<>();
		assessmentMappingStrategy.setType(StarfishAssessment.class);
		assessmentMappingStrategy.setColumnMapping(StarfishAssessment.HEADER);

		ColumnPositionMappingStrategy<StarfishScore> scoreMappingStrategy = new StarfishScoreMappingStrategy<>();
		scoreMappingStrategy.setType(StarfishScore.class);
		scoreMappingStrategy.setColumnMapping(StarfishScore.HEADER);
		
		boolean useProvider = serverConfigurationService.getBoolean("starfish.use.provider", false);

		try (
				BufferedWriter assessmentWriter = Files.newBufferedWriter(assessmentFile, StandardCharsets.UTF_8);
				BufferedWriter scoreWriter = Files.newBufferedWriter(scoreFile, StandardCharsets.UTF_8);
			) {

			StatefulBeanToCsv<StarfishAssessment> assessmentBeanToCsv = new StatefulBeanToCsvBuilder<StarfishAssessment>(assessmentWriter)
				.withMappingStrategy(assessmentMappingStrategy)
				.build();

			StatefulBeanToCsv<StarfishScore> scoreBeanToCsv = new StatefulBeanToCsvBuilder<StarfishScore>(scoreWriter)
					.withMappingStrategy(scoreMappingStrategy)
					.build();

			List<StarfishAssessment> saList = new ArrayList<>();
			List<StarfishScore> scList = new ArrayList<>();
	
			// Loop through all terms provided in sakai.properties
			for (String termEid : termEids) {
	
				List<Site> sites = getSites(termEid);
				log.info("Sites to process for term " + termEid + ": " + sites.size());
	
				for (Site s : sites) {
					String siteId = s.getId();
					Map<String, Set<String>> providerUserMap = new HashMap<>();

					if (useProvider) {
						String unpackedProviderId = StringUtils.trimToNull(s.getProviderGroupId());
						if (unpackedProviderId == null) continue;
						String[] providers = groupProvider.unpackId(unpackedProviderId);
						// Section section = courseManagementService.getSection(providerId);
						//EnrollmentSet es = section.getEnrollmentSet();
						
						for (String providerId : providers) {
							Set<Membership> sm = courseManagementService.getSectionMemberships(providerId);
							Set<String> providerUsers = new HashSet<>();
							for (Membership m : sm) {
								providerUsers.add(m.getUserId());
								log.debug("user: {}, status: {}", m.getUserId(), m.getStatus());
							}
							providerUserMap.put(providerId, providerUsers);
						}
					}
					log.debug("Processing site: {} - {}, useProvider: {}", siteId, s.getTitle(), useProvider);

					//get users in site, skip if none
					List<User> users = getValidUsersInSite(siteId);
					if(users == null || users.isEmpty()) {
						log.info("No users in site: {}, skipping", siteId);
						continue;
					}
	
					//get gradebook for this site, skip if none
					Gradebook gradebook = null;
					List<Assignment> assignments = new ArrayList<Assignment>();
	
					try {
						gradebook = (Gradebook)gradebookService.getGradebook(siteId);
	
						//get list of assignments in gradebook, skip if none
						assignments = gradebookService.getAssignments(gradebook.getUid());
						if(assignments == null || assignments.isEmpty()) {
							log.debug("No assignments for site: {}, skipping", siteId);
							continue;
						}
						log.debug("Assignments for site ({}) size: {}", siteId, assignments.size());
						
						for (Assignment a : assignments) {
							String gbIntegrationId = siteId + "-" + a.getId();
							String description = a.getExternalAppName() != null ? "From " + a.getExternalAppName() : "";
							String dueDate = a.getDueDate() != null ? dateFormatter.format(a.getDueDate()) : "";
							int isCounted = a.isCounted() ? 1 : 0;
							
							if (!providerUserMap.isEmpty()) {
								// Write out one CSV row per section (provider)
								for (String p : providerUserMap.keySet()) {
									gbIntegrationId = p + "-" + a.getId();
									StarfishAssessment sa = new StarfishAssessment(gbIntegrationId, p, a.getName(), description, dueDate, a.getPoints().toString(), isCounted, 0, 0);
									log.debug("StarfishAssessment: {}", sa.toString());
									saList.add(sa);
								}
							}
							else {
								saList.add(new StarfishAssessment(gbIntegrationId, siteId, a.getName(), description, dueDate, a.getPoints().toString(), isCounted, 0, 0));
							}
	
							// for each user, get the assignment results for each assignment
							for (User u : users) {
								GradeDefinition gd = gradebookService.getGradeDefinitionForStudentForItem(gradebook.getUid(), a.getId(), u.getId());
						
								if (gd != null && gd.getDateRecorded() != null && gd.getGrade() != null) {
									String gradedTimestamp = tsFormatter.format(gd.getDateRecorded());

									if (!providerUserMap.isEmpty()) {
										for (Entry<String, Set<String>> e : providerUserMap.entrySet()) {
											String providerId = e.getKey();
											Set<String> usersInProvider = e.getValue();
											String userEid = u.getEid();
											
											if (usersInProvider.contains(userEid)) {
												scList.add(new StarfishScore(gbIntegrationId, providerId, userEid, gd.getGrade(), "", gradedTimestamp));
											}
										}
									}
									else {
										scList.add(new StarfishScore(gbIntegrationId, siteId, u.getEid(), gd.getGrade(), "", gradedTimestamp));
									}
								}
							}
						}

						String courseGradeId = siteId + "-CG";
						if (!providerUserMap.isEmpty()) {
							// Write out one CSV row per section (provider)
							for (String p : providerUserMap.keySet()) {
								courseGradeId = p + "-CG";
								saList.add(new StarfishAssessment(courseGradeId, p, "Course Grade", "Calculated Course Grade", "", "100", 0, 1, 1));
							}
						}
						else {
							saList.add(new StarfishAssessment(courseGradeId, siteId, "Course Grade", "Calculated Course Grade", "", "100", 0, 1, 1));
						}

						// Get the final course grades. Note the map has eids.
						Map<String, String> courseGrades = gradebookService.getImportCourseGrade(gradebook.getUid(), true, false);
						for (Map.Entry<String, String> entry : courseGrades.entrySet()) {
							final String userEid = entry.getKey();
							final String userGrade = entry.getValue();

							if (userGrade != null && !userGrade.equals("0.0")) {
								BigDecimal bd = new BigDecimal(userGrade);
								bd = bd.setScale(2, BigDecimal.ROUND_HALF_UP);
								final String roundedGrade = bd.toString();

								if (!providerUserMap.isEmpty()) {
									for (Entry<String, Set<String>> e : providerUserMap.entrySet()) {
										String providerId = e.getKey();
										Set<String> usersInProvider = e.getValue();
										
										if (usersInProvider.contains(userEid)) {
											scList.add(new StarfishScore(courseGradeId, providerId, userEid, roundedGrade, "", nowTimestamp));
										}
									}
								}
								else {
									scList.add(new StarfishScore(courseGradeId, siteId, userEid, roundedGrade, "", nowTimestamp));
								}
							}
						}
					} catch (GradebookNotFoundException gbe) {
						log.info("No gradebook for site: " + siteId + ", skipping.");
						continue;
					} catch (Exception e) {
						log.error("Problem while processing gbExport for site: " + siteId, e);
						continue;
					}
				}
			}

			// Sort the two lists
			Collections.sort(saList, Comparator.comparing(StarfishAssessment::getIntegration_id));
			Collections.sort(scList,
					Comparator.comparing(StarfishScore::getGradebook_item_integration_id)
					.thenComparing(StarfishScore::getCourse_section_integration_id)
					.thenComparing(StarfishScore::getUser_integration_id)
			);

			// Write the entire list of objects out to CSV
			assessmentBeanToCsv.write(saList);
			scoreBeanToCsv.write(scList);
		} catch (IOException e) {
			log.error("Could not start writer", e);
		} catch (CsvDataTypeMismatchException e) {
			log.error("Csv mismatch", e);
		} catch (CsvRequiredFieldEmptyException e) {
			log.error("Missing required field for CSV", e);
		}

		log.info(JOB_NAME + " ended.");
	}
	
	
	/**
	 * Start a session for the admin user and the given jobName
	 */
	private void establishSession(String jobName) {
		
		//set the user information into the current session
	    Session sakaiSession = sessionManager.getCurrentSession();
	    sakaiSession.setUserId("admin");
	    sakaiSession.setUserEid("admin");

	    //establish the user's session
	    usageSessionService.startSession("admin", "127.0.0.1", "starfish-export");
	
	    //update the user's externally provided realm definitions
	    authzGroupService.refreshUser("admin");

	    //post the login event
	    eventTrackingService.post(eventTrackingService.newEvent(UsageSessionService.EVENT_LOGIN, null, true));
	}
	
	
	/**
	 * Get configurable output path. Defaults to /tmp
	 * @return
	 */
	private String getOutputPath() {
		return serverConfigurationService.getString("starfish.export.path", FileUtils.getTempDirectoryPath());
	}
	
	/**
	 * Get all sites that match the criteria, filter out special sites and my workspace sites
	 * @return
	 */
	private List<Site> getSites(String termEid) {

		//setup property criteria
		//this could be extended to dynamically fill the map with properties and values from sakai.props
		Map<String, String> propertyCriteria = new HashMap<String,String>();
		propertyCriteria.put("term_eid", termEid);

		List<Site> sites = new ArrayList<Site>();
			
		List<Site> allSites = siteService.getSites(SelectionType.ANY, null, null, propertyCriteria, SortType.ID_ASC, null);		
		
		for(Site s: allSites) {
			//filter my workspace
			if(siteService.isUserSite(s.getId())){
				continue;
			}
			
			//filter special sites
			if(siteService.isSpecialSite(s.getId())){
				continue;
			}
			
			log.debug("Site: " + s.getId());
			
			//otherwise add it
			sites.add(s);
		}
		
		return sites;
	}
	
	
	/**
	 * Get the users of a site that have the relevant permission
	 * @param siteId
	 * @return list or null if site is bad
	 */
	private List<User> getValidUsersInSite(String siteId) {
		
		try {
			
			Set<String> userIds = siteService.getSite(siteId).getUsersIsAllowed("gradebook.viewOwnGrades");			
			return userDirectoryService.getUsers(userIds);

		} catch (IdUnusedException e) {
			return null;
		} catch (Exception e) {
			log.warn("Error retrieving users", e);
			return null;
		}
		
	}
	
	/**
	 * Helper to delete a file. Will only delete files, not directories.
	 * @param assessmentFile	path to file to delete.
	 * @return
	 */
	private boolean deleteFile(Path p) {
		try {
			File f = p.toFile();
			
			//if doesn't exist, return true since we don't need to delete it
			if(!f.exists()) {
				return true;
			}
			
			//check it is a file and delete it
			if(f.isFile()) {
				return f.delete();
			}
			return false;
		} catch (Exception e) {
			return false;
		}
		
	}
	
	/**
	 * Get the most recent active term
	 * @return
	 */
	private String[] getCurrentTerms() {
		Set<String> termSet = new HashSet<>();
		
		List<AcademicSession> sessions = courseManagementService.getCurrentAcademicSessions();
		
		log.debug("terms: " + sessions.size());

		if(sessions.isEmpty()) {
			return null;
		}
				
		for(AcademicSession as: sessions) {
			termSet.add(as.getEid());
			log.debug("term: " + as.getEid());
		}
		
		return termSet.toArray(new String[termSet.size()]);

	}
	
}

class StarfishAssessmentMappingStrategy<T> extends ColumnPositionMappingStrategy<T> {

    @Override
    public String[] generateHeader() {
        return StarfishAssessment.HEADER;
    }
}

class StarfishScoreMappingStrategy<T> extends ColumnPositionMappingStrategy<T> {

    @Override
    public String[] generateHeader() {
        return StarfishScore.HEADER;
    }
}


/**
 * Comparator class for sorting a grade map by its value
 */
class ValueComparator implements Comparator<String> {

    Map<String, Double> base;
    public ValueComparator(Map<String, Double> base) {
        this.base = base;
    }

    public int compare(String a, String b) {
        if (base.get(a) >= base.get(b)) {
            return -1;
        } else {
            return 1;
        }
    }
}
