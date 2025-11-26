package weshare.controller;

import io.javalin.http.Handler;
import weshare.model.Expense;
import weshare.model.PaymentRequest;
import weshare.model.Person;
import weshare.persistence.ExpenseDAO;
import weshare.server.Routes;
import weshare.server.ServiceRegistry;
import weshare.server.WeShareServer;

import javax.money.MonetaryAmount;
import java.time.LocalDate;
import java.util.Collection;
import java.util.Map;

public class PaymentRequestsReceivedController {
    public static final Handler view = context -> {
        ExpenseDAO expenseDAO = ServiceRegistry.lookup(ExpenseDAO.class);
        Person person = WeShareServer.getPersonLoggedIn(context);
        Collection<PaymentRequest> paymentRequestsReceived = expenseDAO.findPaymentRequestsReceived(person);

        double grandTotal = 0;
        for (PaymentRequest paymentRequest : paymentRequestsReceived) {
            MonetaryAmount paymentAmount = paymentRequest.getAmountToPay();
            double payment = paymentAmount.getNumber().doubleValue();
            grandTotal += payment;
        }
        String fomattedGrandTotal1 = String.format("ZAR %.2f", grandTotal);
        String grand_total = fomattedGrandTotal1.split(",")[0];
        Map<String, Object> viewModel = Map.of("paymentRequestsReceived", paymentRequestsReceived, "grand_total", grand_total);
        context.render("paymentRequestsReceived.html", viewModel);
    };

    public static final Handler submit = context -> {
        ExpenseDAO expenseDAO = ServiceRegistry.lookup(ExpenseDAO.class);
        Person person = WeShareServer.getPersonLoggedIn(context);
        Collection<PaymentRequest> paymentRequestsReceived = expenseDAO.findPaymentRequestsReceived(person);
        String paymentid = context.formParamAsClass("payment",String.class).get();

        for (PaymentRequest paymentRequest : paymentRequestsReceived) {
            if (String.valueOf(paymentRequest.getId()).equals(paymentid)) {
                paymentRequest.pay(person, LocalDate.now());
                Expense paymentExpense = new Expense(person, paymentRequest.getDescription(), paymentRequest.getAmountToPay(), LocalDate.now());
                expenseDAO.save(paymentExpense);
            }
        }
        context.redirect(Routes.paymentRequestReceived);
    };
}
