package weshare.controller;

import io.javalin.http.Handler;
import weshare.model.PaymentRequest;
import weshare.model.Person;
import weshare.persistence.ExpenseDAO;
import weshare.server.ServiceRegistry;
import weshare.server.WeShareServer;

import javax.money.MonetaryAmount;
import java.util.Collection;
import java.util.Map;

public class PaymentRequestSentController {
    public static final Handler view = context -> {
        ExpenseDAO expenseDAO = ServiceRegistry.lookup(ExpenseDAO.class);
        Person person = WeShareServer.getPersonLoggedIn(context);
        Collection<PaymentRequest> paymentRequestsSent = expenseDAO.findPaymentRequestsSent(person);

        double grandTotal = 0;
        for (PaymentRequest paymentRequest : paymentRequestsSent) {
            MonetaryAmount paymentAmount = paymentRequest.getAmountToPay();
            double payment = paymentAmount.getNumber().doubleValue();
            grandTotal += payment;
        }
        String fomattedGrandTotal1 = String.format("ZAR %.2f", grandTotal);
        String grand_total = fomattedGrandTotal1.split(",")[0];

        Map<String,Object> viewModel = Map.of("paymentRequestsSent", paymentRequestsSent, "grand_total", grand_total);
        context.render("paymentRequestsSent.html", viewModel);
    };
}
