package weshare.server;

import weshare.controller.*;

import static io.javalin.apibuilder.ApiBuilder.get;
import static io.javalin.apibuilder.ApiBuilder.post;

public class Routes {
    public static final String LOGIN_PAGE = "/";
    public static final String LOGIN_ACTION = "/login.action";
    public static final String ADD_EXPENSE_ACTION = "/expense.action";
    public static final String LOGOUT = "/logout";
    public static final String EXPENSES = "/expenses";
    public static final String NEWEXPENSES = "/newexpense";
    public static final String NEW_PAYMENT_REQUEST_FORM = "/paymentrequest";
    public static final String ADD_PAYMENTREQUEST_ACTION = "/paymentrequest.action";
    public static final String paymentRequestSent = "/paymentrequests_sent";
    public static final String paymentRequestReceived = "/paymentrequests_received";
    public static final String ADD_PAYFORM_ACTION = "/payment.action";

    public static void configure(WeShareServer server) {
        server.routes(() -> {
            post(LOGIN_ACTION,  PersonController.login);
            post(ADD_EXPENSE_ACTION, AddExpenseActionController.submit);
            post(ADD_PAYMENTREQUEST_ACTION, NewPaymentRequestFormController.submit);
            post(ADD_PAYFORM_ACTION, PaymentRequestsReceivedController.submit);
            get(LOGOUT,         PersonController.logout);
            get(EXPENSES,           ExpensesController.view);
            get(NEWEXPENSES,           NewExpenseFormController.view);
            get(NEW_PAYMENT_REQUEST_FORM,           NewPaymentRequestFormController.view);
            get(paymentRequestSent,           PaymentRequestSentController.view);
            get(paymentRequestReceived,           PaymentRequestsReceivedController.view);
        });
    }
}
