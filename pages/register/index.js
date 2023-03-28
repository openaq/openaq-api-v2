import './style.scss';

import { zxcvbn, zxcvbnOptions } from '@zxcvbn-ts/core';
import zxcvbnCommonPackage from '@zxcvbn-ts/language-common';
import zxcvbnEnPackage from '@zxcvbn-ts/language-en';

const options = {
  translations: zxcvbnEnPackage.translations,
  graphs: zxcvbnCommonPackage.adjacencyGraphs,
  dictionary: {
    ...zxcvbnCommonPackage.dictionary,
    ...zxcvbnEnPackage.dictionary,
  },
};

zxcvbnOptions.setOptions(options);

const nameInput = document.querySelector('.js-name-input');
const emailInput = document.querySelector('.js-email-input');
const passwordInput = document.querySelector('.js-password-input');
const passwordConfirmInput = document.querySelector(
  '.js-password-confirm-input'
);
const passwordMatchWarning = document.querySelector(
  '.js-password-match-warning'
);
const submitBtn = document.querySelector('.js-submit-btn');

let score;

nameInput.addEventListener('input', () => {
  checkFormFieldsComplete()
})

emailInput.addEventListener('input', () => {
  checkFormFieldsComplete()
})

passwordInput.addEventListener('input', (e) => {
  const result = zxcvbn(e.target.value);
  score = result.score;
  const strengthMessage = document.querySelector(
    '.js-strength-message'
  );
  strengthMessage.innerText = '';
  resetBars();
  // only show bars if password field has value
  if (e.target.value.length > 0) {
    setPasswordStrength(result);
  }
  if (passwordConfirmInput.value != '') {
    passwordMatchOnInput()
  }
  checkFormFieldsComplete();
});

let passwordMatchTimeout;

passwordConfirmInput.addEventListener('input', () => {
  passwordMatchOnInput();
  checkFormFieldsComplete();
});

function resetBars() {
  const bars = document.querySelectorAll('.strength-meter__bar');
  for (const bar of bars) {
    bar.classList.remove(...bar.classList);
    bar.classList.add(`strength-meter__bar`);
  }
}

function passwordMatchOnInput() {
  clearTimeout(passwordMatchTimeout);
  passwordMatchTimeout = setTimeout(
    () =>
      checkPasswordsMatch(
        passwordInput.value,
        passwordConfirmInput.value
      ),
    300
  );
}

function checkPasswordsMatch(password, confirmPassword) {
  if (password != confirmPassword) {
    passwordMatchWarning.innerText = 'Passwords do not match.';
  } else {
    passwordMatchWarning.innerText = '';
  }
}

let formfieldsCheckTimeout;

function checkFormFieldsComplete() {
  clearTimeout(formfieldsCheckTimeout)
  formfieldsCheckTimeout = setTimeout(() => {
    if (nameInput.value != '' && emailInput.value != '' && passwordInput.value != '' && passwordConfirmInput.value != '' && passwordInput.value == passwordConfirmInput.value && score > 3) {
      submitBtn.disabled = false;
    } else {
      submitBtn.disabled = true;
    }
  }, 300)


}

function setPasswordStrength(result) {
  let color;
  let message;
  let warning = result.feedback.warning;
  switch (result.score) {
    case 0:
      color = 'warning';
      message = `Very weak password ${warning}`;
      break;
    case 1:
      color = 'warning';
      message = `Weak password ${warning}`;
      break;
    case 2:
      color = 'alert';
      message = 'Somewhat secure password';
      break;
    case 3:
      color = 'ok';
      message = 'Strong password';
      break;
    case 4:
      color = 'ok';
      message = 'Very strong password';
      break;
    default:
      color = 'red';
      message = 'Weak password';
  }
  const strengthMessage = document.querySelector(
    '.js-strength-message'
  );
  strengthMessage.innerText = message;
  const bars = document.querySelectorAll('.strength-meter__bar');
  let barsArr = Array.prototype.slice.call(bars);
  barsArr = barsArr.splice(0, result.score + 1);
  for (const bar of barsArr) {
    bar.classList.add(`bg-status-${color}`);
  }
}
