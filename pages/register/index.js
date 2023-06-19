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

let passwordInputTimeout;

const errorSvg =
  '<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 -960 960 960"><path fill="#dd443c" d="M480-280q17 0 29-11t11-29q0-17-11-28t-29-12q-17 0-28 12t-12 28q0 17 12 29t28 11Zm-40-160h80v-240h-80v240Zm40 360q-83 0-156-31t-127-86q-54-54-85-127T80-480q0-83 32-156t85-127q54-54 127-85t156-32q83 0 156 32t127 85q54 54 86 127t31 156q0 83-31 156t-86 127q-54 54-127 86T480-80Zm0-80q134 0 227-93t93-227q0-134-93-227t-227-93q-134 0-227 93t-93 227q0 134 93 227t227 93Zm0-320Z"/></svg>';
const checkSvg =
  '<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 -960 960 960"><path fill="#89c053" d="M382-240 154-468l57-57 171 171 367-367 57 57-424 424Z"/></svg>';

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
  checkFormFieldsComplete();
});

emailInput.addEventListener('input', () => {
  checkFormFieldsComplete();
});

passwordInput.addEventListener('input', (e) => {
  clearTimeout(passwordInputTimeout);
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
    passwordMatchOnInput();
  }
  if (result.score < 3) {
    e.target.setCustomValidity(
      'password not strong enough, please choose a stronger password.'
    );
  } else {
    e.target.setCustomValidity('');
  }
  if (e.target.value.length === 0) {
    e.target.setCustomValidity('');
  }
  checkFormFieldsComplete();
  passwordInputTimeout = setTimeout(
    () => e.target.reportValidity(),
    1000
  );
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
  while (passwordMatchWarning.firstChild) {
    passwordMatchWarning.removeChild(passwordMatchWarning.firstChild);
  }
  if (password != confirmPassword) {
    passwordMatchWarning.insertAdjacentHTML('afterbegin', errorSvg);
    passwordMatchWarning.innerText = 'Passwords do not match.';
  } else {
    passwordMatchWarning.innerText = '';
  }
}

let formfieldsCheckTimeout;

function checkFormFieldsComplete() {
  clearTimeout(formfieldsCheckTimeout);
  formfieldsCheckTimeout = setTimeout(() => {
    if (
      nameInput.value != '' &&
      emailInput.value != '' &&
      passwordInput.value != '' &&
      passwordConfirmInput.value != '' &&
      passwordInput.value == passwordConfirmInput.value &&
      score >= 3
    ) {
      submitBtn.disabled = false;
    } else {
      submitBtn.disabled = true;
    }
  }, 300);
}

function setPasswordStrength(result) {
  let color;
  let message;
  let symbol;

  switch (result.score) {
    case 0:
      color = 'warning';
      symbol = errorSvg;
      message = `Very weak password`;
      break;
    case 1:
      color = 'warning';
      symbol = errorSvg;
      message = `Weak password`;
      break;
    case 2:
      color = 'alert';
      symbol = errorSvg;
      message = `Somewhat secure password`;
      break;
    case 3:
      color = 'ok';
      message = 'Strong password';
      symbol = checkSvg;
      break;
    case 4:
      color = 'ok';
      message = 'Very strong password';
      symbol = checkSvg;
      break;
    default:
      color = 'red';
      symbol = errorSvg;
      message = 'Weak password';
  }
  const strengthMessage = document.querySelector(
    '.js-strength-message'
  );
  const strengthWarning = document.querySelector(
    '.js-strength-warning'
  );
  while (strengthMessage.firstChild) {
    strengthMessage.removeChild(strengthMessage.firstChild);
  }
  strengthMessage.innerText = message;
  strengthMessage.insertAdjacentHTML('afterbegin', symbol);
  strengthWarning.innerText = result.feedback.warning;
  const bars = document.querySelectorAll('.strength-meter__bar');
  let barsArr = Array.prototype.slice.call(bars);
  barsArr = barsArr.splice(0, result.score + 1);
  for (const bar of barsArr) {
    bar.classList.add(`strength-meter__bar--${color}`);
  }
}
