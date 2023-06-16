import './style.scss';


const emailInput = document.querySelector('.js-email-input');
const passwordInput = document.querySelector('.js-password-input');

const submitBtn = document.querySelector('.js-submit-btn');

emailInput.addEventListener('input', () => {
  checkFormFieldsComplete()
})

passwordInput.addEventListener('input', (e) => {
  checkFormFieldsComplete();
});

let formfieldsCheckTimeout;

function checkFormFieldsComplete() {
  clearTimeout(formfieldsCheckTimeout)
  formfieldsCheckTimeout = setTimeout(() => {
    if (emailInput.value != '' && passwordInput.value != '') {
      submitBtn.disabled = false;
    } else {
      submitBtn.disabled = true;
    }
  }, 300)
}
