package dist_copy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

/**
 * An ApplicationMaster for executing shell commands on a set of launched containers using the YARN
 * framework.
 * 
 * <p>
 * This class is meant to act as an example on how to write yarn-based application masters.
 * </p>
 * 
 * <p>
 * The ApplicationMaster is started on a container by the <code>ResourceManager</code>'s launcher.
 * The first thing that the <code>ApplicationMaster</code> needs to do is to connect and register
 * itself with the <code>ResourceManager</code>. The registration sets up information within the
 * <code>ResourceManager</code> regarding what host:port the ApplicationMaster is listening on to
 * provide any form of functionality to a client as well as a tracking url that a client can use to
 * keep track of status/job history if needed. However, in the distributedshell, trackingurl and
 * appMasterHost:appMasterRpcPort are not supported.
 * </p>
 * 
 * <p>
 * The <code>ApplicationMaster</code> needs to send a heartbeat to the <code>ResourceManager</code>
 * at regular intervals to inform the <code>ResourceManager</code> that it is up and alive. The
 * {@link ApplicationMasterProtocol#allocate} to the <code>ResourceManager</code> from the
 * <code>ApplicationMaster</code> acts as a heartbeat.
 * 
 * <p>
 * For the actual handling of the job, the <code>ApplicationMaster</code> has to request the
 * <code>ResourceManager</code> via {@link AllocateRequest} for the required no. of containers using
 * {@link ResourceRequest} with the necessary resource specifications such as node location,
 * computational (memory/disk/cpu) resource requirements. The <code>ResourceManager</code> responds
 * with an {@link AllocateResponse} that informs the <code>ApplicationMaster</code> of the set of
 * newly allocated containers, completed containers as well as current state of available resources.
 * </p>
 * 
 * <p>
 * For each allocated container, the <code>ApplicationMaster</code> can then set up the necessary
 * launch context via {@link ContainerLaunchContext} to specify the allocated container id, local
 * resources required by the executable, the environment to be setup for the executable, commands to
 * execute, etc. and submit a {@link StartContainerRequest} to the
 * {@link ContainerManagementProtocol} to launch and execute the defined commands on the given
 * allocated container.
 * </p>
 * 
 * <p>
 * The <code>ApplicationMaster</code> can monitor the launched container by either querying the
 * <code>ResourceManager</code> using {@link ApplicationMasterProtocol#allocate} to get updates on
 * completed containers or via the {@link ContainerManagementProtocol} by querying for the status of
 * the allocated container's {@link ContainerId}.
 * 
 * <p>
 * After the job has been completed, the <code>ApplicationMaster</code> has to send a
 * {@link FinishApplicationMasterRequest} to the <code>ResourceManager</code> to inform it that the
 * <code>ApplicationMaster</code> has been completed.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ApplicationMaster {
    private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);
    private final Configuration conf;
    // Handle to communicate with the Resource Manager
    @SuppressWarnings("rawtypes")
    private AMRMClientAsync amRMClient;
    // Handle to communicate with the Node Manager
    private NMClientAsync nmClientAsync;
    // Listen to process the response from the Node Manager
    private NMCallbackHandler containerListener;
    // Application Attempt Id ( combination of attemptId and fail count )
    private ApplicationAttemptId appAttemptId;

    // For status update for clients - yet to be implemented
    // Hostname of the container
    private String appMasterHostname = "";
    // Port on which the app master listens for status updates from clients
    private final int appMasterRpcPort = -1;
    // Tracking url to which app master publishes info for clients to monitor
    private final String appMasterTrackingUrl = "";

    // App Master configuration
    // No. of containers to run //TODO: check
    private int numTotalContainers = 1;
    // Memory to request for the container on which the shell command will run
    private int containerMemory = 10;// TODO: check
    // Priority of the request
    private int requestPriority;// TODO: check

    // Counter for completed containers ( complete denotes successful or failed )
    private final AtomicInteger numCompletedContainers = new AtomicInteger();
    // Allocated container count so that we know how many containers has the RM
    // allocated to us
    private final AtomicInteger numAllocatedContainers = new AtomicInteger();
    // Count of failed containers
    private final AtomicInteger numFailedContainers = new AtomicInteger();
    // Count of containers already requested from the RM
    // Needed as once requested, we should not request for containers again.
    // Only request for more if the original requirement changes.
    private final AtomicInteger numRequestedContainers = new AtomicInteger();

    private final Map<String, String> cntnrEnv = new HashMap<String, String>(); // TODO: check

    private volatile boolean done;
    private volatile boolean success;

    private ByteBuffer allTokens;

    // Launch threads
    private final CompletionService<Void> completionService = new ExecutorCompletionService<Void>(
            Executors.newCachedThreadPool());

    /**
     * @param args Command line args
     */
    public static void main(String[] args) {
        boolean result = false;
        try {
            final ApplicationMaster appMaster = new ApplicationMaster();
            LOG.info("Initializing ApplicationMaster");
            final boolean run = appMaster.init(args);
            if (!run) {
                System.exit(0);
            }
            result = appMaster.run();
        } catch (final Throwable t) {
            LOG.fatal("Error running ApplicationMaster", t);
            System.exit(1);
        }
        if (result) {
            LOG.info("Application Master completed successfully. exiting");
            System.exit(0);
        } else {
            LOG.info("Application Master failed. exiting");
            System.exit(2);
        }
    }

    /**
     * Dump out contents of $CWD and the environment to stdout for debugging
     */
    private static void dumpOutDebugInfo() {
        LOG.info("Dump debug output");
        final Map<String, String> envs = System.getenv();
        for (final Map.Entry<String, String> env : envs.entrySet()) {
            LOG.info("System env: key: " + env.getKey() + ", val: " + env.getValue());
            System.out.println("System env: key: " + env.getKey() + ", val: "
                    + env.getValue());
        }

        final String cmd = "ls -al";
        final Runtime run = Runtime.getRuntime();
        Process pr = null;
        try {
            pr = run.exec(cmd);
            pr.waitFor();

            final BufferedReader buf = new BufferedReader(new InputStreamReader(
                    pr.getInputStream()));
            String line = "";
            while ((line = buf.readLine()) != null) {
                LOG.info("System CWD content: " + line);
                System.out.println("System CWD content: " + line);
            }
            buf.close();
        } catch (final IOException e) {
            e.printStackTrace();
        } catch (final InterruptedException e) {
            e.printStackTrace();
        }
    }

    public ApplicationMaster() {
        // Set up the configuration
        conf = new YarnConfiguration();
    }

    /**
     * Parse command line options
     * 
     * @param args Command line args
     * @return Whether init successful and run should be invoked
     * @throws ParseException
     */
    public boolean init(String[] args) throws ParseException {
        final Options opts = new Options();
        opts.addOption("container_memory", true, "Amount of memory in MB to be requested to?");
        opts.addOption("priority", true, "Application Priority. Default 0");
        opts.addOption("debug", false, "Dump out debug information");
        opts.addOption("help", false, "Print usage");

        final CommandLine cliParser = new GnuParser().parse(opts, args);
        if (args.length == 0) {
            printUsage(opts);
            throw new IllegalArgumentException("No args specified for application master to initialize");
        }

        if (cliParser.hasOption("help")) {
            printUsage(opts);
            return false;
        }

        if (cliParser.hasOption("debug")) {
            dumpOutDebugInfo();
        }

        final Map<String, String> envs = System.getenv();

        if (!envs.containsKey(Environment.CONTAINER_ID.name())) {
            if (cliParser.hasOption("app_attempt_id")) {
                final String appIdStr = cliParser.getOptionValue("app_attempt_id", "");
                appAttemptId = ConverterUtils.toApplicationAttemptId(appIdStr);
            } else {
                throw new IllegalArgumentException("Application Attempt Id not set in the environment");
            }
        } else {
            final ContainerId containerId = ConverterUtils.toContainerId(envs.get(Environment.CONTAINER_ID.name()));
            appAttemptId = containerId.getApplicationAttemptId();
        }

        if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
            throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV + " not set in the environment");

        }
        if (!envs.containsKey(Environment.NM_HOST.name())) {
            throw new RuntimeException(Environment.NM_HOST.name() + " not set in the environment");
        }
        if (!envs.containsKey(Environment.NM_HTTP_PORT.name())) {
            throw new RuntimeException(Environment.NM_HTTP_PORT + " not set in the environment");
        }
        if (!envs.containsKey(Environment.NM_PORT.name())) {
            throw new RuntimeException(Environment.NM_PORT.name() + " not set in the environment");
        }

        LOG.info("Application master for app, appId: "
                + appAttemptId.getApplicationId().getId() + ", clustertimestamp: "
                + appAttemptId.getApplicationId().getClusterTimestamp()
                + ", attemptId: " + appAttemptId.getAttemptId());

        // TODO: check setup env for dist-copy?

        containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "10"));
        numTotalContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));
        if (numTotalContainers == 0) {
            throw new IllegalArgumentException("Cannot run distributed shell with no containers");
        }
        requestPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
        return true;
    }

    /**
     * Helper function to print usage
     * 
     * @param opts Parsed command line options
     */
    private static void printUsage(Options opts) {
        new HelpFormatter().printHelp("ApplicationMaster", opts);
    }

    /**
     * Main run function for the application master
     * 
     * @throws YarnException
     * @throws IOException
     */
    @SuppressWarnings({"unchecked"})
    public boolean run() throws YarnException, IOException {
        LOG.info("Starting ApplicationMaster");
        final Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
        // Now remove the AM->RM token so that containers cannot access it.
        final Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
        while (iter.hasNext()) {
            final Token<?> token = iter.next();
            if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
                iter.remove();
            }
        }
        // TODO: file bug on DS.
        final DataOutputBuffer dob = new DataOutputBuffer();
        credentials.writeTokenStorageToStream(dob);
        allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

        final AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
        amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
        amRMClient.init(conf);
        amRMClient.start();

        containerListener = createNMCallbackHandler();
        nmClientAsync = new NMClientAsyncImpl(containerListener);
        nmClientAsync.init(conf);
        nmClientAsync.start();

        // Setup local RPC Server to accept status requests directly from clients
        // TODO need to setup a protocol for client to be able to communicate to
        // the RPC server
        // TODO use the rpc port info to register with the RM for the client to
        // send requests to this app master

        // Register self with ResourceManager
        // This will start sending heart-beat to the RM
        appMasterHostname = NetUtils.getHostname();
        final RegisterApplicationMasterResponse response = amRMClient.registerApplicationMaster(
                appMasterHostname, appMasterRpcPort, appMasterTrackingUrl);

        // Dump out information about cluster capability as seen by the
        // resource manager
        final int maxMem = response.getMaximumResourceCapability().getMemory();
        LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

        // A resource ask cannot exceed the max.
        if (containerMemory > maxMem) {
            LOG.info("Container memory specified above max threshold of cluster."
                    + " Using max value." + ", specified: " + containerMemory + ", max: "
                    + maxMem);
            containerMemory = maxMem;
        }

        // Setup ask for containers from RM
        // Send request for containers to RM
        // Until we get our fully allocated quota, we keep on polling RM for containers
        // Keep looping until all the containers are launched and ?
        // executed on them ( regardless of success/failure).
        for (int i = 0; i < numTotalContainers; i++) {
            final ContainerRequest containerAsk = setupContainerAskForRM();
            amRMClient.addContainerRequest(containerAsk);
        }
        numRequestedContainers.set(numTotalContainers);
        while (!done && (numCompletedContainers.get() != numTotalContainers)) {
            try {
                Thread.sleep(200);
            } catch (final InterruptedException ex) {}
        }
        finish();
        return success;
    }

    @VisibleForTesting
    NMCallbackHandler createNMCallbackHandler() {
        return new NMCallbackHandler(this);
    }

    private void finish() throws YarnException, IOException {
        // Join all launched threads
        // needed for when we time out
        // and we need to release containers
        for (int i = 0; i < numTotalContainers; i++) {
            try {
                final Future<Void> f = completionService.take();
                f.get();
            } catch (final InterruptedException e) {
                throw new YarnException(e);
            } catch (final ExecutionException e) {
                throw new YarnException("Container failed." + e.getCause(), e);
            }
        }

        // When the application completes, it should stop all running containers
        LOG.info("Application completed. Stopping running containers");
        nmClientAsync.stop();

        // When the application completes, it should send a finish application
        // signal to the RM
        LOG.info("Application completed. Signalling finish to RM");

        FinalApplicationStatus appStatus;
        String appMessage = null;
        success = true;
        if (numFailedContainers.get() == 0 &&
                numCompletedContainers.get() == numTotalContainers) {
            appStatus = FinalApplicationStatus.SUCCEEDED;
        } else {
            appStatus = FinalApplicationStatus.FAILED;
            appMessage = "Diagnostics." + ", total: " + numTotalContainers
                    + ", completed: " + numCompletedContainers.get() + ", allocated: "
                    + numAllocatedContainers.get() + ", failed: "
                    + numFailedContainers.get();
            success = false;
        }
        amRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
        amRMClient.stop();
    }

    private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
        @SuppressWarnings("unchecked")
        @Override
        public void onContainersCompleted(List<ContainerStatus> completedContainers) {
            LOG.info("Got response from RM for container ask, completedCnt: " + completedContainers.size());
            for (final ContainerStatus containerStatus : completedContainers) {
                LOG.info("Got container status for container-Id: "
                        + containerStatus.getContainerId() + ", state: "
                        + containerStatus.getState() + ", exitStatus: "
                        + containerStatus.getExitStatus() + ", diagnostics: "
                        + containerStatus.getDiagnostics());
                // non-complete containers should not be here
                assert (containerStatus.getState() == ContainerState.COMPLETE);
                // increment counters for completed/failed containers
                final int exitStatus = containerStatus.getExitStatus();
                if (exitStatus != 0) {
                    // container failed
                    if (exitStatus != ContainerExitStatus.ABORTED) {
                        numCompletedContainers.incrementAndGet();
                        numFailedContainers.incrementAndGet();
                    } else {
                        // TODO: write test-case to simulate container failure
                        // Note: how to restart specific containers on failure?
                        // container was killed by framework, possibly preempted
                        // we should re-try as the container was lost for some reason
                        numAllocatedContainers.decrementAndGet();
                        numRequestedContainers.decrementAndGet();
                        // we do not need to release the container as it would be done by the RM
                    }
                } else {
                    // nothing to do, container completed successfully
                    numCompletedContainers.incrementAndGet();
                    LOG.info("Container completed successfully." + ", containerId: " + containerStatus.getContainerId());
                }
            }

            // ask for more containers if any failed
            final int askCount = numTotalContainers - numRequestedContainers.get();
            numRequestedContainers.addAndGet(askCount);

            if (askCount > 0) {
                for (int i = 0; i < askCount; ++i) {
                    final ContainerRequest containerAsk = setupContainerAskForRM();
                    amRMClient.addContainerRequest(containerAsk);
                }
            }

            if (numCompletedContainers.get() == numTotalContainers) {
                done = true;
            }
        }

        @Override
        public void onContainersAllocated(List<Container> allocatedContainers) {
            LOG.info("Got response from RM for container ask, allocated-cnt: " + allocatedContainers.size());
            numAllocatedContainers.addAndGet(allocatedContainers.size());
            for (final Container allocatedContainer : allocatedContainers) {
                LOG.info("Launching shell command on a new container."
                        + ", containerId: " + allocatedContainer.getId()
                        + ", containerNode: " + allocatedContainer.getNodeId().getHost()
                        + ":" + allocatedContainer.getNodeId().getPort()
                        + ", containerNodeUri: " + allocatedContainer.getNodeHttpAddress()
                        + ", containerResourceMemory"
                        + allocatedContainer.getResource().getMemory());
                final LaunchContainer runnableLaunchContainer =
                        new LaunchContainer(allocatedContainer, containerListener);
                completionService.submit(runnableLaunchContainer);
            }
        }

        @Override
        public void onShutdownRequest() {
            done = true;
        }

        @Override
        public void onNodesUpdated(List<NodeReport> updatedNodes) {}

        @Override
        public float getProgress() {
            // set progress to deliver to RM on next heartbeat
            final float progress = (float) numCompletedContainers.get()
                    / numTotalContainers;
            return progress;
        }

        @Override
        public void onError(Throwable e) {
            done = true;
            LOG.error(e.getMessage(), e);
            amRMClient.stop();
        }
    }

    @VisibleForTesting
    static class NMCallbackHandler implements NMClientAsync.CallbackHandler {
        private final ConcurrentMap<ContainerId, Container> containers =
                new ConcurrentHashMap<ContainerId, Container>();
        private final ApplicationMaster applicationMaster;
        
        public NMCallbackHandler(ApplicationMaster applicationMaster) {
            this.applicationMaster = applicationMaster;
        }

        public void addContainer(ContainerId containerId, Container container) {
            containers.putIfAbsent(containerId, container);
        }

        @Override
        public void onContainerStopped(ContainerId containerId) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Succeeded to stop Container " + containerId);
            }
            containers.remove(containerId);
        }

        @Override
        public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Container Status: id: " + containerId + ", status: " + containerStatus);
            }
        }

        @Override
        public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Succeeded to start Container " + containerId);
            }
            final Container container = containers.get(containerId);
            if (container != null) {
                applicationMaster.nmClientAsync.getContainerStatusAsync(containerId, container.getNodeId());
            }
        }

        @Override
        public void onStartContainerError(ContainerId containerId, Throwable t) {
            LOG.error("Failed to start Container " + containerId, t);
            containers.remove(containerId);
            applicationMaster.numCompletedContainers.incrementAndGet();
            applicationMaster.numFailedContainers.incrementAndGet();
        }

        @Override
        public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
            LOG.error("Failed to query the status of Container " + containerId, t);
        }

        @Override
        public void onStopContainerError(ContainerId containerId, Throwable t) {
            LOG.error("Failed to stop Container " + containerId, t);
            containers.remove(containerId);
        }
    }

    /**
     * Thread to connect to the {@link ContainerManagementProtocol} and launch the container that
     * will execute the shell command.
     */
    private class LaunchContainer implements Callable<Void> {
        // Allocated container
        Container container;
        NMCallbackHandler containerListener;

        /**
         * @param lcontainer Allocated container
         * @param containerListener Callback handler of the container
         */
        public LaunchContainer(Container container, NMCallbackHandler containerListener) {
            this.container = container;
            this.containerListener = containerListener;
        }

        @Override
        /**
         * Connects to CM, sets up container launch context and eventually dispatches the container  
         * start request to the CM. 
         */
        public Void call() {
            LOG.info("Setting up container launch container for container-id: " + container.getId());
            final ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
            // Set the environment
            ctx.setEnvironment(cntnrEnv);
            // Set the local resources
            final Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
            ctx.setLocalResources(localResources);

            // Set the necessary command to execute on the allocated container
            final Vector<CharSequence> vargs = new Vector<CharSequence>(5);

            // Add log redirect params
            vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
            vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

            // Get final commmand
            final StringBuilder command = new StringBuilder();
            for (final CharSequence str : vargs) {
                command.append(str).append(" ");
            }

            final List<String> commands = new ArrayList<String>();
            commands.add(command.toString());
            ctx.setCommands(commands);

            // Set up tokens for the container too. Today, for normal shell commands,
            // the container in distribute-shell doesn't need any tokens. We are
            // populating them mainly for NodeManagers to be able to download any
            // files in the distributed file-system. The tokens are otherwise also
            // useful in cases, for e.g., when one is running a "hadoop dfs" command
            // inside the distributed shell.
            ctx.setTokens(allTokens.duplicate());

            containerListener.addContainer(container.getId(), container);
            nmClientAsync.startContainerAsync(container, ctx);
            return null;
        }
    }

    /**
     * Setup the request that will be sent to the RM for the container ask.
     * 
     * @return the setup ResourceRequest to be sent to RM
     */
    private ContainerRequest setupContainerAskForRM() {
        // setup requirements for hosts
        // using * as any host will do for the distributed shell app
        // set the priority for the request
        final Priority pri = Records.newRecord(Priority.class);
        // TODO - what is the range for priority? how to decide?
        pri.setPriority(requestPriority);
        // Set up resource type requirements
        // For now, only memory is supported so we set memory requirements
        final Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(containerMemory);
        final ContainerRequest request = new ContainerRequest(capability, null, null, pri);
        LOG.info("Requested container ask: " + request.toString());
        return request;
    }
}
