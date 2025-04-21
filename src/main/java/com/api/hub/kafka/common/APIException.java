package com.api.hub.kafka.common;

public class APIException extends Exception {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String customMessage;
    private String errorMessage;
    private int errorCode;
    private String stackTraceString;

    public APIException(String customMessage, String errorMessage, int errorCode) {
        super(errorMessage); // Store in Exception's message field
        this.customMessage = customMessage;
        this.errorMessage = errorMessage;
        this.errorCode = errorCode;
        this.stackTraceString = captureStackTrace();
    }

    private String captureStackTrace() {
        StringBuilder sb = new StringBuilder();
        for (StackTraceElement element : this.getStackTrace()) {
            sb.append("\tat ").append(element.toString()).append("\n");
        }
        return sb.toString();
    }

    public String getCustomMessage() {
        return customMessage;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public String getStackTraceString() {
        return stackTraceString;
    }

    @Override
    public String toString() {
        return "APIException {\n" +
               "  customMessage='" + customMessage + '\'' + ",\n" +
               "  errorMessage='" + errorMessage + '\'' + ",\n" +
               "  errorCode=" + errorCode + ",\n" +
               "  stackTrace=\n" + stackTraceString +
               '}';
    }
}