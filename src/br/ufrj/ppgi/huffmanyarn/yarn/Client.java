/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package br.ufrj.ppgi.huffmanyarn.yarn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;


//public class Client {
public class Client {
	private static final Log LOG = LogFactory.getLog(Client.class);

	// YarnClient
	private YarnClient yarnClient;
	
	// File to be compressed
	private String fileName;


	public Client(String[] args) throws Exception {
		this.fileName = args[0];
	}

	public boolean run() throws YarnException, IOException {
		Configuration conf = new YarnConfiguration();
		
		yarnClient = YarnClient.createYarnClient();
		yarnClient.init(conf);
		yarnClient.start();
		
		// Get a new application id
		YarnClientApplication app = yarnClient.createApplication();

		// Set the application context
		ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
		ApplicationId appId = appContext.getApplicationId();
		appContext.setKeepContainersAcrossApplicationAttempts(false);
		appContext.setApplicationName("HuffmanYarn");
		
		// Set up the container launch context for the application master
		ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

		// Set up resource requirements, priority and queue
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(10);
		capability.setVirtualCores(1);
		appContext.setResource(capability);
		Priority pri = Records.newRecord(Priority.class);
		pri.setPriority(0);
		appContext.setPriority(pri);
		appContext.setQueue("default");
				
		// Set container for context
		appContext.setAMContainerSpec(amContainer);

		// Instance of FileSystem
		FileSystem fs = FileSystem.get(conf);
		
		// Set local resources for the application master local files or archives as needed. In this scenario, the jar file for the application master is part of the local resources
		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
		
		// Copy the application master jar to the filesystem. Create a local resource to point to the destination jar path
		LOG.info("Copying AppMaster jar from local filesystem and add to local environment");
		addToLocalResources(fs, "huffmanyarn.jar", "AppMaster.jar", appId.toString(), localResources, null);
		
		// Set local resource info into app master container launch context
		amContainer.setLocalResources(localResources);

		// Set the env variables to be setup in the env where the application master will be run
		LOG.info("Set the environment for the application master");
		Map<String, String> env = new HashMap<String, String>();

		// Env variable CLASSPATH
		StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$()).append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
		for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
			classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
			classPathEnv.append(c.trim());
		}
		env.put("CLASSPATH", classPathEnv.toString());

		// Set env
		amContainer.setEnvironment(env);

		// Set the necessary command to execute the application master
		Vector<CharSequence> vargs = new Vector<CharSequence>(30);

		// Set java executable command
		vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
		//vargs.add("/usr/bin/echo $CLASSPATH");
		
		// Set java args
		vargs.add("-Xmx" + 10 + "m");
		vargs.add(ApplicationMaster.class.getName());
		vargs.add(appId.toString());
		vargs.add(this.fileName);
		vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
		vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

		// Get final commmand
		StringBuilder command = new StringBuilder();
		for (CharSequence str : vargs) {
			command.append(str).append(" ");
		}

		LOG.info("Completed setting up app master command " + command.toString());
		List<String> commands = new ArrayList<String>();
		commands.add(command.toString());
		amContainer.setCommands(commands);

		// Submit the application to the applications manager. SubmitApplicationResponse submitResp = applicationsManager.submitApplication(appRequest); Ignore the response as either a valid response object is returned on success or an exception thrown to denote some form of a failure
		LOG.info("Submitting application to ASM");
		yarnClient.submitApplication(appContext);

		// Monitor the application
		return monitorApplication(appId);
	}

	/**
	 * Monitor the submitted application for completion. Kill application if
	 * time expires.
	 * 
	 * @param appId
	 *            Application Id of application to be monitored
	 * @return true if application completed successfully
	 * @throws YarnException
	 * @throws IOException
	 */
	private boolean monitorApplication(ApplicationId appId)
			throws YarnException, IOException {

		while (true) {

			// Check app status every 1 second.
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				LOG.debug("Thread sleep in monitoring loop interrupted");
			}

			// Get application report for the appId we are interested in 
			ApplicationReport report = yarnClient.getApplicationReport(appId);

			LOG.info("Got application report from ASM for" + ", appId="
					+ appId.getId() + ", clientToAMToken="
					+ report.getClientToAMToken() + ", appDiagnostics="
					+ report.getDiagnostics() + ", appMasterHost="
					+ report.getHost() + ", appQueue=" + report.getQueue()
					+ ", appMasterRpcPort=" + report.getRpcPort()
					+ ", appStartTime=" + report.getStartTime()
					+ ", yarnAppState="
					+ report.getYarnApplicationState().toString()
					+ ", distributedFinalState="
					+ report.getFinalApplicationStatus().toString()
					+ ", appTrackingUrl=" + report.getTrackingUrl()
					+ ", appUser=" + report.getUser());

			YarnApplicationState state = report.getYarnApplicationState();
			FinalApplicationStatus dsStatus = report
					.getFinalApplicationStatus();
			if (YarnApplicationState.FINISHED == state) {
				if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
					LOG.info("Application has completed successfully. Breaking monitoring loop");
					return true;
				} else {
					LOG.info("Application did finished unsuccessfully."
							+ " YarnState=" + state.toString()
							+ ", DSFinalStatus=" + dsStatus.toString()
							+ ". Breaking monitoring loop");
					return false;
				}
			} else if (YarnApplicationState.KILLED == state
					|| YarnApplicationState.FAILED == state) {
				LOG.info("Application did not finish." + " YarnState="
						+ state.toString() + ", DSFinalStatus="
						+ dsStatus.toString() + ". Breaking monitoring loop");
				return false;
			}
		}
	}


	private void addToLocalResources(FileSystem fs, String fileSrcPath, String fileDstPath, String appId, Map<String, LocalResource> localResources, String resources)
			throws IOException {
		String suffix = "HuffmanYarn/" + appId + "/" + fileDstPath;
		Path dst = new Path(fs.getHomeDirectory(), suffix);
		if (fileSrcPath == null) {
			FSDataOutputStream ostream = null;
			try {
				ostream = FileSystem.create(fs, dst, new FsPermission(
						(short) 0710));
				ostream.writeUTF(resources);
			} finally {
				IOUtils.closeQuietly(ostream);
			}
		} else {
			fs.copyFromLocalFile(new Path(fileSrcPath), dst);
		}
		FileStatus scFileStatus = fs.getFileStatus(dst);
		LocalResource scRsrc = LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(dst.toUri()), LocalResourceType.FILE, LocalResourceVisibility.APPLICATION, scFileStatus.getLen(), scFileStatus.getModificationTime());
		localResources.put(fileDstPath, scRsrc);
	}
	
	
//	/**
//	 * @param args
//	 *            Command line arguments
//	 * @throws Exception 
//	 */
//	public static void main(String[] args) throws Exception {
//		Client client = new Client(args);
//		
//		if (client.run()) {
//			LOG.info("Compressão completa!");
//		}
//		else {
//			LOG.error("Erro durante a compressão");
//		}
//		
//		System.exit(0);
//	}
}
