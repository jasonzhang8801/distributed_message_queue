import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jasonzhang on 5/29/17.
 */

public class DSBS {
    // DSBS's ip and port number
    public static String ipAddr;
    public static int portNum;
    // DSBS's data storage
    public static ConcurrentHashMap<String, ConcurrentHashMap<Integer, List<Record>>> dataMap;
    // DSBS's cluster information
    public static List<String[]> brokerList;

    public DSBS() {
        ipAddr = null;
        portNum = -1;
        dataMap = new ConcurrentHashMap<>();
        brokerList = new ArrayList<>();
    }

    /**
     * create the information map to store the topic with the partitions
     */
    public static void createInfoMap() {}


    public static void main(String[] args) {

    }


}

/**
 * server
 */
class DSBSServer implements Runnable {
    // DSBS server's ip and port
    private String ipAddr = null;
    private int portNum = -1;

    @Override
    public void run() {

        try (ServerSocket serverSocket = new ServerSocket(0);) {
            // assign ip and port number
            ipAddr = InetAddress.getLocalHost().getHostAddress();
            portNum = serverSocket.getLocalPort();

            if (ipAddr != null && portNum > 0) {
                DSBS.ipAddr = ipAddr;
                DSBS.portNum = portNum;

                // add local host's network info to brokerList
                DSBS.brokerList.add(new String[]{ipAddr, Integer.toString(portNum)});

            } else {
                System.out.println("Error: no valid IP or port number");
            }



            // listen the request
            while (true) {
                (new Thread(new DSBSServerWorker(serverSocket.accept()))).start();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

/**
 * server worker
 */
class DSBSServerWorker implements Runnable {

    private Socket clientSocket = null;

    DSBSServerWorker(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
        try (ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());
        ) {
            // create received package
            Package revPkg = null;

            try {
                if ((revPkg = (Package) in.readObject()) != null) {

                    TYPE pkgType = revPkg._type;

                    switch (pkgType) {
                        case B2BADD: {
                            // add the broker cluster
                            System.out.println("Server: receive package with type \"B2BADD\"");

                            // retrieve the brokerList
                            B2BAdd pkg = (B2BAdd) revPkg;
                            DSBS.brokerList = pkg._brokerList;
                            pkg._ack = true;

                            // send ACK back
                            out.writeObject(pkg);
                            System.out.println("Server: send ACK back");

                        }
                        case T2B: {
                            // create topic into the broker cluster
                            break;
                        }
                        default: {
                            break;
                        }
                    }
                }
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

/**
 * console client to read stdin from the user
 */
class DSBSClient {

    public void setUp() {
        // read user input
        try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in));) {
            String state = "idle";
            DSBSParserEntry parserEntry = null;

            while (true) {
                if (state == null) {
                    System.out.println("System error: state shouldn't be null");
                    return;
                }

                switch (state.toLowerCase()) {
                    case "idle": {
                        // read user input
                        System.out.print("broker> ");
                        String stdIn = br.readLine();

                        // check the input
                        Pattern pattern = Pattern.compile("^\\s*$");
                        Matcher matcher = pattern.matcher(stdIn);
                        if (matcher.find()) {
                            System.out.println("Error: please type the valid command");
                            break;
                        }

                        // parse user's input
                        parserEntry = DSBSUtility.parser(stdIn);

                        //
                        if (parserEntry == null) {
                            System.out.println("System error: invalid parsing result, try again");
                            break;
                        }

                        // change the state
                        state = parserEntry.commandName;
                        break;
                    }
                    case "add": {
                        // construct the list of broker with ip and port
                        List<String> listOfIpAddr = parserEntry.listOfIpAddr;
                        List<String> listOfPortNum = parserEntry.listOfPortNum;

                        // check number of host
                        if (listOfIpAddr.size() <= 0 ||
                                listOfPortNum.size() <= 0 ||
                                listOfIpAddr.size() != listOfPortNum.size()) {
                            System.out.println("System error: the number of ip and port should be matched and positive");

                            state = "idle";
                            break;
                        }

                        // merge brokerList
                        for (int i = 0; i < listOfIpAddr.size(); i++) {
                            DSBS.brokerList.add(new String[]{listOfIpAddr.get(i), listOfPortNum.get(i)});
                        }

                        for (int i = 0; i < listOfIpAddr.size(); i++) {
                            String remoteIpAddr = listOfIpAddr.get(i);
                            int remotePortNum = Integer.parseInt(listOfPortNum.get(i));

                            try (Socket socket = new Socket(remoteIpAddr, remotePortNum)) {
                                try (ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

                                    // construct the B2BAdd package
                                    B2BAdd sendPkg = new B2BAdd(TYPE.B2BADD, DSBS.brokerList);

                                    // send out to remote broker
                                    out.writeObject(sendPkg);
                                    System.out.println("Client: send merged brokerList to remote host with IP " +
                                            remoteIpAddr);

                                    // construct the received package
                                    Package revPkg = null;

                                    try {
                                        if ((revPkg = (Package) in.readObject()) != null) {
                                            if (revPkg._type == TYPE.B2BADD && revPkg._ack) {
                                                System.out.println("Client: add remote broker with IP " + remoteIpAddr);
                                            } else {
                                                System.out.println("Error: failed to add remote broker with IP " + remoteIpAddr);
                                            }
                                        }
                                    } catch (ClassNotFoundException e) {
                                        e.printStackTrace();
                                    }
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                                System.out.println("Client: invalid IP or port number");
                                System.exit(0);
                            }
                        }

                        state = "idle";
                        break;
                    }
                    default: {
                        break;
                    }
                }
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class DSBSParserEntry {
    // command name
    String commandName = null;

    // add command
    List<String> listOfIpAddr = null;
    List<String> listOfPortNum = null;
}

abstract class DSBSUtility {

    public static DSBSParserEntry parser(String in) {
        DSBSParserEntry parserEntry = new DSBSParserEntry();

        // check input
        if (in == null || in.length() == 0) return null;

        // trim white space
        String trimmed = in.trim();

        // regular express
        Pattern pattern = null;
        Matcher matcher = null;

        // check the command
        pattern = Pattern.compile("^(add)");
        matcher = pattern.matcher(trimmed);

        if (!matcher.find()) {
            // no valid command
            System.out.println("Error: no valid command");
            return null;
        } else {
            // check if the parentheses are valid
            if (!DSBSUtility.isValidParenthesis(trimmed)) {
                System.out.println("Error: invalid parentheses");
                return null;
            }

            // retrieve command name
            String commandName = matcher.group(1);
            parserEntry.commandName = commandName;

            String withoutCommandSubstr = trimmed.substring(matcher.end(1)).trim();

            switch (commandName.toLowerCase()) {
                case "add": {
                    // init the remote network information list
                    parserEntry.listOfIpAddr = new ArrayList<>();
                    parserEntry.listOfPortNum = new ArrayList<>();

                    // retrieve the valid network information
                    // e.g. "123.456.78.90, 1234"
                    int start_pos = 0;
                    for (int i = 0; i < withoutCommandSubstr.length(); i++) {
                        char c = withoutCommandSubstr.charAt(i);

                        if (c == '(') {
                            start_pos = i;
                        } else if (c == ')') {
                            String content = withoutCommandSubstr.substring(start_pos + 1, i);

                            String remoteHostName = null;
                            String remoteIpAddr = null;
                            String remotePortNum = null;

                            // validate the remote host name
                            pattern = Pattern.compile("(^[a-zA-Z][a-zA-Z0-9]*)");
                            matcher = pattern.matcher(content);

                            if (matcher.find()) {
                                remoteHostName = matcher.group(1);
                            } else {
                                System.out.println("Error: invalid host name");
                                System.out.println("Help: please review the following host naming convention");
                                System.out.println("Host name must start with English letters");
                                System.out.println("Host name only contains English letters and numbers");
                                System.out.println("Please type command \"help\" to get more details");
                                return null;
                            }

                            // validate the remote host ip
                            pattern = Pattern.compile("(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})");
                            matcher = pattern.matcher(content);

                            if (matcher.find()) {
                                remoteIpAddr = matcher.group(1);
                            } else {
                                System.out.println("Error: invalid host ip");
                                System.out.println("Help: please review the following IP address convention");
                                System.out.println("123.123.12.12");
                                System.out.println("Please type command \"help\" to get more details");
                                return null;
                            }

                            // validate the remote host port
                            pattern = Pattern.compile(",\\s*(\\d{4,5}$)");
                            matcher = pattern.matcher(content);

                            if (matcher.find() && (Integer.parseInt(matcher.group(1)) >= 1024 && Integer.parseInt(matcher.group(1)) <= 49151)) {
                                remotePortNum = matcher.group(1);
                            } else {
                                System.out.println("Error: invalid host port");
                                System.out.println("Help: valid port number should be between 1024 and 49151, inclusive");
                                System.out.println("Please type command \"help\" to get more details");
                                return null;
                            }

                            // NEED TO CHECK THE SEPARATOR
//                            // validate the separator, comma
//                            pattern = Pattern.compile(".+,\\s*.+,\\s*.+");
//                            matcher = pattern.matcher(content);
//                            if (!matcher.find())

                            // add ip and port number
                            parserEntry.listOfIpAddr.add(remoteIpAddr);
                            parserEntry.listOfPortNum.add(remotePortNum);
                        }
                    }
                    break;
                }
                default: {
                    System.out.println("System error: invalid command");
                    break;
                }
            }
        }

        return parserEntry; 
    }

    private static boolean isValidParenthesis(String in) {
        // check input
        if (in == null || in.length() == 0) return false;

        // check if there is at least one parenthesis
        Pattern pattern = Pattern.compile("[\\(\\)]");
        Matcher matcher = pattern.matcher(in);
        if (!matcher.find()) return false;

        // create a stack to store the parenthesis
        Deque<Character> stack = new ArrayDeque<>();

        for (int i = 0; i < in.length(); i++) {
            char c = in.charAt(i);

            // push the '(' into the stack
            if (c == '(') {
                stack.push(in.charAt(i));
            }
            // pop the ')' from the stack
            else if (c == ')') {
                if (stack.isEmpty()) {
                    return false;
                }
                stack.pop();
            }
        }

        if (!stack.isEmpty()) return false;
        return true;
    }
}
