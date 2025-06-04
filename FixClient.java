package com.boci.marblefix;

import quickfix.*;
import quickfix.field.*;
import quickfix.field.TimeUnit;
import quickfix.fix44.Logon;
import quickfix.MessageUtils;
import java.io.*;
import java.util.concurrent.*;

public class FixClient implements Application {
    private SessionID sessionId;
    private ScheduledExecutorService executorService;
    private long lastFilePosition = 0;
    private File messagesFile = new File("messages.txt");

    @Override
    public void onCreate(SessionID sessionId) {}

    @Override
    public void onLogon(SessionID sessionId) {
        System.out.println("Logged in!");
        this.sessionId = sessionId;
        try {
			initializeSequenceNumbers();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        startFileWatcher();
    }

    private void initializeSequenceNumbers() throws IOException {
        Session session = Session.lookupSession(sessionId);
		session.setNextSenderMsgSeqNum(2);
		session.setNextTargetMsgSeqNum(2);
    }

    @Override
    public void toAdmin(Message message, SessionID sessionId) {
        if (message instanceof Logon) {
            message.setField(new ResetSeqNumFlag(true));
        }
    }

    private void startFileWatcher() {
        executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {

               
                    checkFileUpdates();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, 0, 1, java.util.concurrent.TimeUnit.SECONDS);
    }

    private void checkFileUpdates() throws Exception {
        if (!messagesFile.exists()) return;

        try (RandomAccessFile raf = new RandomAccessFile(messagesFile, "r")) {
            if (raf.length() < lastFilePosition) {
                lastFilePosition = 0;
            }
            
            raf.seek(lastFilePosition);
            String line;
            while ((line = raf.readLine()) != null) {
                System.out.println("File updated");
                processMessageLine(line);
            }
            lastFilePosition = raf.getFilePointer();
        }
    }

    private void processMessageLine(String line) {
        try {
            DataDictionary dd = new DataDictionary("FIX44.xml");
            String fixMessage = line.replace('|', '\u0001');
            
            Message message = MessageUtils.parse(
                new DefaultMessageFactory(), dd, fixMessage
            );

            message.getHeader().removeField(MsgSeqNum.FIELD);
            message.getHeader().removeField(BodyLength.FIELD);
            message.getHeader().removeField(CheckSum.FIELD);

            message.getHeader().setField(new SenderCompID(sessionId.getSenderCompID()));
            message.getHeader().setField(new TargetCompID(sessionId.getTargetCompID()));
            System.out.println("Message ready to send:" + message.toXML());
            Session.sendToTarget(message, sessionId);

            System.out.println("Sent message with seq: " + 
					Session.lookupSession(sessionId).getStore().getNextSenderMsgSeqNum());

            
        } catch (Exception e) {
            System.err.println("Error processing message: " + e.getMessage());
        }
    }

    @Override
    public void onLogout(SessionID sessionId) {
        System.out.println("Logged out!");
        executorService.shutdown();
    }

    @Override
    public void fromAdmin(Message message, SessionID sessionId) {}
    @Override
    public void fromApp(Message message, SessionID sessionId) {}
    @Override
    public void toApp(Message message, SessionID sessionId) {}

    public static void main(String[] args) throws Exception {
        SessionSettings settings = new SessionSettings("fix-client.cfg");
        Application application = new FixClient();
        MessageStoreFactory storeFactory = new FileStoreFactory(settings);
        LogFactory logFactory = new ScreenLogFactory(true, true, true);
        MessageFactory messageFactory = new DefaultMessageFactory();

        Initiator initiator = new SocketInitiator(
            application, storeFactory, settings, logFactory, messageFactory
        );

        initiator.start();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                initiator.stop();
            }
        }));
        Thread.currentThread().join();
    }
}